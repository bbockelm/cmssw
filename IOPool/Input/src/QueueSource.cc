/*----------------------------------------------------------------------
----------------------------------------------------------------------*/

#include "AMQPQueue.h"
#include "QueueSource.h"
#include "InputFile.h"
#include "RootFile.h"
#include "DuplicateChecker.h"
#include "RunHelper.h"

#include "TSystem.h"

#include "DataFormats/Common/interface/ThinnedAssociation.h"
#include "DataFormats/Provenance/interface/BranchDescription.h"
#include "DataFormats/Provenance/interface/IndexIntoFile.h"
#include "DataFormats/Provenance/interface/ProductRegistry.h"
#include "DataFormats/Provenance/interface/ThinnedAssociationsHelper.h"
#include "FWCore/Framework/interface/EventPrincipal.h"
#include "FWCore/Framework/interface/FileBlock.h"
#include "FWCore/Framework/interface/InputSourceDescription.h"
#include "FWCore/Framework/interface/LuminosityBlockPrincipal.h"
#include "FWCore/Framework/src/PreallocationConfiguration.h"
#include "FWCore/Framework/src/SharedResourcesRegistry.h"
#include "FWCore/Framework/interface/SharedResourcesAcquirer.h"
#include "FWCore/Framework/interface/RunPrincipal.h"
#include "FWCore/MessageLogger/interface/MessageLogger.h"
#include "FWCore/ParameterSet/interface/ConfigurationDescriptions.h"
#include "FWCore/ParameterSet/interface/ParameterSetDescription.h"
#include "FWCore/ServiceRegistry/interface/ActivityRegistry.h"
#include "FWCore/Sources/interface/EventSkipperByID.h"
#include "FWCore/Utilities/interface/EDMException.h"
#include "FWCore/Utilities/interface/Exception.h"
#include "FWCore/Utilities/interface/InputType.h"

#include <set>

namespace edm {

  class BranchID;
  class LuminosityBlockID;
  class EventID;
  class ThinnedAssociationsHelper;

  QueueSource::QueueSource(ParameterSet const& pset, InputSourceDescription const& desc) :
    InputSource(pset, desc),
    rootServiceChecker_(),
    branchIDsToReplace_(),
    nStreams_(desc.allocations_->numberOfStreams()),
    skipBadFiles_(pset.getUntrackedParameter<bool>("skipBadFiles")),
    bypassVersionCheck_(pset.getUntrackedParameter<bool>("bypassVersionCheck")),
    treeMaxVirtualSize_(pset.getUntrackedParameter<int>("treeMaxVirtualSize")),
    productSelectorRules_(pset, "inputCommands", "InputSource"),
    dropDescendants_(pset.getUntrackedParameter<bool>("dropDescendantsOfDroppedBranches")),
    labelRawDataLikeMC_(pset.getUntrackedParameter<bool>("labelRawDataLikeMC")),
    runHelper_(makeRunHelper(pset)),
    resourceSharedWithDelayedReaderPtr_(),
    eventSkipperByID_(EventSkipperByID::create(pset).release()),
    duplicateChecker_(new DuplicateChecker(pset)),
    initialNumberOfEventsToSkip_(pset.getUntrackedParameter<unsigned int>("skipEvents")),
    noEventSort_(pset.getUntrackedParameter<bool>("noEventSort")),
    treeCacheSize_(noEventSort_ ? pset.getUntrackedParameter<unsigned int>("cacheSize") : 0U),
    overrideCatalogLocation_(pset.getUntrackedParameter<std::string>("overrideCatalog", std::string())),
    fileQueue_(new AMQPQueue(pset))
  {
    auto resources = SharedResourcesRegistry::instance()->createAcquirerForSourceDelayedReader();
    resourceSharedWithDelayedReaderPtr_ = std::make_unique<SharedResourcesAcquirer>(std::move(resources.first));
    mutexSharedWithDelayedReader_ = resources.second;

    // Open first file.
    std::string nextCatalogItem;
    bool gotLastFile = true;
    while (!(gotLastFile = !fileQueue_->next(nextCatalogItem))) {
      indexesIntoFiles_.emplace_back(nullptr);
      std::vector<std::string> filenames = {nextCatalogItem};
      InputFileCatalog catalog(filenames, overrideCatalogLocation_);
      fileCatalogItem_ = catalog.fileCatalogItems()[0];
      fileSequenceNum++;
      initFile();
      if (rootFile()) {
        gotLastFile = false;
        break;
      }
    }
    // fileSequenceNum will point to:
    // * -1 if there were no files available in the queue.
    // *  0 if the first file returned by queue was usable.
    // *  1 if the second file was usable, the first one was unusable, and skipBadFiles is on.
    // *  2 ... and so on.
    if (gotLastFile) {
      lastFileSequenceNum = fileSequenceNum;
    } else {
      productRegistryUpdate().updateFromInput(rootFile()->productRegistry()->productList());
      if (initialNumberOfEventsToSkip_ != 0) {
        skipEvents(initialNumberOfEventsToSkip_);
      }
    }

    std::shared_ptr<ActivityRegistry> ar = actReg();
    if (!ar) {
      edm::Exception ex(edm::errors::OtherCMS);
      ex << "Activity registry unavailable when queue source was constructed.";
      ex.addContext("QueueSource::QueueSource()");
      throw ex;
    }
    ar->watchJobFailure(this, &QueueSource::jobFailure);
    ar->watchPostEndJob(this, &QueueSource::jobSuccess);
  }

  QueueSource::~QueueSource() {}

  void
  QueueSource::endJob() {
    closeFile_();
    InputFile::reportReadBranches();
  }

  std::unique_ptr<FileBlock>
  QueueSource::readFile_() {
    // First file is a special case as the constructor setup the file.
    // Afterward, each time the event processor wants to read a file, we should
    // move the iterator forward and return the result.
    if (readFileInvokedOnce_) nextFile();
    readFileInvokedOnce_ = true;

    if(!rootFile()) {
      return std::make_unique<FileBlock>();
    }
    return rootFile()->createFileBlock();
  }

  void QueueSource::closeFile_() {
    auto &file = rootFile();
    if(file) {
      auto sentry = std::make_unique<InputSource::FileCloseSentry>(*this, lfn(), usedFallback());
      filesToAck_.push_back(lfn());
      file->close();
      file.reset();
    }
  }

  std::shared_ptr<RunAuxiliary>
  QueueSource::readRunAuxiliary_() {
    assert(rootFile());
    return rootFile()->readRunAuxiliary_();
  }

  std::shared_ptr<LuminosityBlockAuxiliary>
  QueueSource::readLuminosityBlockAuxiliary_() {
    assert(rootFile());
    return rootFile()->readLuminosityBlockAuxiliary_();
  }

  void
  QueueSource::readRun_(RunPrincipal& runPrincipal) {
    assert(rootFile());
    rootFile()->readRun_(runPrincipal);
  }

  void
  QueueSource::readLuminosityBlock_(LuminosityBlockPrincipal& lumiPrincipal) {
    assert(rootFile());
    rootFile()->readLuminosityBlock_(lumiPrincipal);
  }

  void
  QueueSource::readEvent_(EventPrincipal& eventPrincipal) {
    assert(rootFile());
    rootFile()->readEvent(eventPrincipal);
  }

  bool
  QueueSource::readIt(EventID const& id, EventPrincipal& eventPrincipal, StreamContext& streamContext) {
    bool found = skipToItem(id.run(), id.luminosityBlock(), id.event());
    if(!found) return false;
    EventSourceSentry sentry(*this, streamContext);
    readEvent_(eventPrincipal);
    return true;
  }

  InputSource::ItemType
  QueueSource::getNextItemType() {
    RunNumber_t run = IndexIntoFile::invalidRun;
    LuminosityBlockNumber_t lumi = IndexIntoFile::invalidLumi;
    EventNumber_t event = IndexIntoFile::invalidEvent;
    InputSource::ItemType itemType = getNextItemTypeFromFile(run, lumi, event);
    return runHelper_->nextItemType(state(), itemType);
  }

  void
  QueueSource::preForkReleaseResources() {
    closeFile_();
  }

  std::pair<SharedResourcesAcquirer*,std::recursive_mutex*>
  QueueSource::resourceSharedWithDelayedReader_() {
    return std::make_pair(resourceSharedWithDelayedReaderPtr_.get(), mutexSharedWithDelayedReader_.get());
  }

  // Advance "offset" events.  Offset can be positive or negative (or zero).
  void
  QueueSource::skip(int offset) {
    skipEvents(offset);
  }

  bool
  QueueSource::goToEvent_(EventID const& eventID) {
    return goToEvent(eventID);
  }

  void
  QueueSource::fillDescriptions(ConfigurationDescriptions & descriptions) {

    ParameterSetDescription desc;

    std::vector<std::string> defaultStrings;
    desc.setComment("Reads EDM/ROOT files determined by a remote queue.");
    desc.addUntracked<std::string>("overrideCatalog", std::string());
    desc.addUntracked<bool>("skipBadFiles", false)
        ->setComment("True:  Ignore any missing or unopenable input file.\n"
                     "False: Throw exception if missing or unopenable input file.");
    desc.addUntracked<bool>("bypassVersionCheck", false)
        ->setComment("True:  Bypass release version check.\n"
                     "False: Throw exception if reading file in a release prior to the release in which the file was written.");
    desc.addUntracked<int>("treeMaxVirtualSize", -1)
        ->setComment("Size of ROOT TTree TBasket cache. Affects performance.");
    desc.addUntracked<bool>("dropDescendantsOfDroppedBranches", true)
        ->setComment("If True, also drop on input any descendent of any branch dropped on input.");
    desc.addUntracked<bool>("labelRawDataLikeMC", true)
        ->setComment("If True: replace module label for raw data to match MC. Also use 'LHC' as process.");

    desc.addUntracked<unsigned int>("skipEvents", 0U)
        ->setComment("Skip the first 'skipEvents' events that otherwise would have been processed.");
    desc.addUntracked<bool>("noEventSort", true)
        ->setComment("True:  Process runs, lumis and events in the order they appear in the file (but see notes 1 and 2).\n"
                     "False: Process runs, lumis and events in each file in numerical order (run#, lumi#, event#) (but see note 3).\n"
                     "Note 1: Events within the same lumi will always be processed contiguously.\n"
                     "Note 2: Lumis within the same run will always be processed contiguously.\n"
                     "Note 3: Any sorting occurs independently in each input file (no sorting across input files).");
    desc.addUntracked<unsigned int>("cacheSize", roottree::defaultCacheSize)
        ->setComment("Size of ROOT TTree prefetch cache.  Affects performance.");

    EventSkipperByID::fillDescription(desc);
    DuplicateChecker::fillDescription(desc);
    ProductSelectorRules::fillDescription(desc, "inputCommands");
    InputSource::fillDescription(desc);
    RunHelperBase::fillDescription(desc);
    AMQPQueue::fillDescription(desc);

    descriptions.add("source", desc);
  }

  bool
  QueueSource::randomAccess_() const {
    return false;
  }

  QueueSource::RootFileSharedPtr
  QueueSource::makeRootFile(std::shared_ptr<InputFile> filePtr) {
      size_t currentIndexIntoFile = sequenceNumberOfFile();
      return std::make_shared<RootFile>(
          fileName(),
          processConfiguration(),
          logicalFileName(),
          filePtr,
          eventSkipperByID(),
          initialNumberOfEventsToSkip_ != 0,
          remainingEvents(),
          remainingLuminosityBlocks(),
          nStreams(),
          treeCacheSize_,
          treeMaxVirtualSize(),
          processingMode(),
          runHelper(),
          noEventSort_,
          productSelectorRules(),
          InputType::Primary,
          branchIDListHelper(),
          thinnedAssociationsHelper(),
          nullptr, // associationsFromSecondary
          duplicateChecker(),
          dropDescendants(),
          processHistoryRegistryForUpdate(),
          indexesIntoFiles(),
          currentIndexIntoFile,
          orderedProcessHistoryIDs_,
          bypassVersionCheck(),
          labelRawDataLikeMC(),
          false, // usingGoToEvent_
          false); // enablePrefetching_
  }

  void
  QueueSource::setIndexIntoFile(size_t index) {
   indexesIntoFiles_[index] = rootFile()->indexIntoFileSharedPtr();
  }

  bool QueueSource::nextFile() {
    if (!noMoreFiles()) {
      fileSequenceNum++;
      std::string nextCatalogItem;
      if (!fileQueue_->next(nextCatalogItem)) {
        // The prior sequence number was actually the last one!
        // fileSequenceNum should now point past the lastFileSequenceNum, and
        // noMoreFiles() should return true.
        lastFileSequenceNum = fileSequenceNum-1;
        closeFile_();  // Invalidate the current file handle as we are past the end.
      } else {
        std::vector<std::string> filenames = {nextCatalogItem};
        InputFileCatalog catalog(filenames, overrideCatalogLocation_);
        fileCatalogItem_ = catalog.fileCatalogItems()[0];
        indexesIntoFiles_.emplace_back(nullptr);
      }
    }
    // This isn't an `else` for the above conditional as the logic above may have changed
    // the return value of this function.
    if (noMoreFiles()) {
      return false;
    }

    initFile();

    // make sure the new product registry is compatible with the main one.
    // This is not done in initFile to allow the constructor to special-case the first file.
    if(rootFile()) {
      std::string mergeInfo = productRegistryUpdate().merge(*rootFile()->productRegistry(),
                                                                   fileName(),
                                                                   BranchDescription::Permissive);
      if(!mergeInfo.empty()) {
        throw Exception(errors::MismatchedInputFiles,"QueueSource::nextFile()") << mergeInfo;
      }
    }

    return true;
  }

  void
  QueueSource::initFile() {
    // If we are not duplicate checking across files and we are not using random access to find events,
    // then we can delete the IndexIntoFile for the file we are closing.
    // If we can't delete all of it, then we can delete the parts we do not need.
    bool deleteIndexIntoFile = !(duplicateChecker_ && duplicateChecker_->checkingAllFiles() && !duplicateChecker_->checkDisabled());

    // We are going to close the open file; delete the old index if it isn't needed.
    if(!noMoreFiles() && (fileSequenceNum > 0)) {
      if(deleteIndexIntoFile) {
        indexesIntoFiles_[fileSequenceNum-1].reset();
      } else if(indexesIntoFiles_[fileSequenceNum-1]) {
        indexesIntoFiles_[fileSequenceNum-1]->inputFileClosed();
      }
    }

    // Perform the close of the current file.
    closeFile_();

    if(noMoreFiles()) {
      // No files specified
      return;
    }

    // Check if the logical file name was found.
    if(fileName().empty()) {
      // LFN not found in catalog.
      InputFile::reportSkippedFile(fileName(), logicalFileName());
      if(!skipBadFiles_) {
        throw cms::Exception("LogicalFileNameNotFound", "QueueSource::initFile()\n")
          << "Logical file name '" << logicalFileName() << "' was not found in the file catalog.\n"
          << "If you wanted a local file, you forgot the 'file:' prefix\n"
          << "before the file name in your configuration file.\n";
      }
      LogWarning("") << "Input logical file: " << logicalFileName() << " was not found in the catalog, and will be skipped.\n";
      return;
    }

    lfn_ = logicalFileName().empty() ? fileName() : logicalFileName();
    lfnHash_ = std::hash<std::string>()(lfn_);
    usedFallback_ = false;

    // Determine whether we have a fallback URL specified; if so, prepare it;
    // Only valid if it is non-empty and differs from the original filename.
    bool hasFallbackUrl = !fallbackFileName().empty() && fallbackFileName() != fileName();

    std::shared_ptr<InputFile> filePtr;
    std::list<std::string> originalInfo;
    try {
      std::unique_ptr<InputSource::FileOpenSentry> sentry(std::make_unique<InputSource::FileOpenSentry>(*this, lfn_, usedFallback_));
      std::unique_ptr<char[]> name(gSystem->ExpandPathName(fileName().c_str()));;
      filePtr = std::make_shared<InputFile>(name.get(), "  Initiating request to open file ", InputType::Primary);
    }
    catch (cms::Exception const& e) {
      if(!skipBadFiles_) {
        if(hasFallbackUrl) {
          std::ostringstream out;
          out << e.explainSelf();

          std::unique_ptr<char[]> name(gSystem->ExpandPathName(fallbackFileName().c_str()));
          std::string pfn(name.get());
          InputFile::reportFallbackAttempt(pfn, logicalFileName(), out.str());
          originalInfo = e.additionalInfo();
        } else {
          InputFile::reportSkippedFile(fileName(), logicalFileName());
          Exception ex(errors::FileOpenError, "", e);
          ex.addContext("Calling QueueSource::initFile()");
          std::ostringstream out;
          out << "Input file " << fileName() << " could not be opened.";
          ex.addAdditionalInfo(out.str());
          throw ex;
        }
      }
    }

    if(!filePtr && (hasFallbackUrl)) {
      try {
        usedFallback_ = true;
        std::unique_ptr<InputSource::FileOpenSentry> sentry(std::make_unique<InputSource::FileOpenSentry>(*this, lfn_, usedFallback_));
        std::unique_ptr<char[]> fallbackFullName(gSystem->ExpandPathName(fallbackFileName().c_str()));
        filePtr.reset(new InputFile(fallbackFullName.get(), "  Fallback request to file ", InputType::Primary));
      }
      catch (cms::Exception const& e) {
        if(!skipBadFiles_) {
          InputFile::reportSkippedFile(fileName(), logicalFileName());
          Exception ex(errors::FallbackFileOpenError, "", e);
          ex.addContext("Calling QueueSource::initFile()");
          std::ostringstream out;
          out << "Input file " << fileName() << " could not be opened.\n";
          out << "Fallback Input file " << fallbackFileName() << " also could not be opened.";
          if (originalInfo.size()) {
            out << std::endl << "Original exception info is above; fallback exception info is below.";
            ex.addAdditionalInfo(out.str());
            for (auto const & s : originalInfo) {
              ex.addAdditionalInfo(s);
            }
          } else {
            ex.addAdditionalInfo(out.str());
          }
          throw ex;
        }
      }
    }
    if(filePtr) {
      rootFile_ = makeRootFile(filePtr);
      rootFile_->setSignals(&(preEventReadFromSourceSignal_), &(postEventReadFromSourceSignal_));
      assert(rootFile_);
      rootFile_->reportOpened("primaryFiles");
    } else {
      InputFile::reportSkippedFile(fileName(), logicalFileName());
      if(!skipBadFiles_) {
        throw Exception(errors::FileOpenError) <<
           "QueueSource::initFile(): Input file " << fileName() << " was not found or could not be opened.\n";
      }
      LogWarning("") << "Input file: " << fileName() << " was not found or could not be opened, and will be skipped.\n";
    }

  }

  InputSource::ItemType
  QueueSource::getNextItemTypeFromFile(RunNumber_t& run, LuminosityBlockNumber_t& lumi, EventNumber_t& event) {
    // The constructor should make sure we are pointing at a valid file (if possible) before
    // the event processor invokes this function.
    if (noMoreFiles()) return InputSource::IsStop;

    // If we have a valid file with entries to process, return one of those.
    if (rootFile()) {
      IndexIntoFile::EntryType entryType = rootFile()->getNextItemType(run, lumi, event);
      if(entryType == IndexIntoFile::kEvent) {
        return InputSource::IsEvent;
      } else if(entryType == IndexIntoFile::kLumi) {
        return InputSource::IsLumi;
      } else if(entryType == IndexIntoFile::kRun) {
        return InputSource::IsRun;
      }
      assert(entryType == IndexIntoFile::kEnd);
      // At this point, the current file is out of entries to process.
      if(atLastFile()) {
        return InputSource::IsStop;
      }
    }
      // The `else` case above is where the current file is not valid but we haven't
      // hit the end-of-queue.
      // Perhaps skipBadFiles is set and the last readFile_ failed to produce a valid file.
      // In that case, we do the same thing as if the prior file ran out of events: return
      // a new file!
    return InputSource::IsFile;
  }

  bool
  QueueSource::skipToItem(RunNumber_t run, LuminosityBlockNumber_t lumi, EventNumber_t event) {
    // Attempt to find item in currently open input file.
    bool found = rootFile() && rootFile()->setEntryAtItem(run, lumi, event);

    // As with RootInputFileSequence, we could maintain a hashmap of previously-read files and search
    // those files; doesn't seem like it is necessary in this case?
    return found;
  }

  void
  QueueSource::jobSuccess() {
    printf("Processing job success.\n");
    for (auto const&file : filesToAck_) {
      fileQueue_->ack(file);
    }
    filesToAck_.clear();
  }

  void
  QueueSource::jobFailure() {
    printf("Processing job failure.\n");
    for (auto const&file : filesToAck_) {
      fileQueue_->nack(file);
    }
    filesToAck_.clear();
  }

}
