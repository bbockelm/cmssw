/*----------------------------------------------------------------------
----------------------------------------------------------------------*/
#include "RootFile.h"
#include "RootInputFileQueue.h"
#include "QueueSource.h"
#include "DuplicateChecker.h"

#include "DataFormats/Provenance/interface/BranchID.h"
#include "DataFormats/Provenance/interface/IndexIntoFile.h"
#include "DataFormats/Provenance/interface/ProductRegistry.h"
#include "FWCore/Framework/interface/FileBlock.h"
#include "FWCore/MessageLogger/interface/MessageLogger.h"
#include "FWCore/ParameterSet/interface/ParameterSet.h"
#include "FWCore/ParameterSet/interface/ParameterSetDescription.h"
#include "FWCore/Sources/interface/EventSkipperByID.h"
#include "Utilities/StorageFactory/interface/StorageFactory.h"

#include "TSystem.h"

namespace edm {
  class BranchIDListHelper;
  class EventPrincipal;
  class LuminosityBlockPrincipal;
  class RunPrincipal;

  RootInputFileQueue::RootInputFileQueue(
                ParameterSet const& pset,
                QueueSource & input,
                InputFileCatalog const& catalog) :
    catalog_(catalog),
    //fileIter_(fileCatalogItems().begin()),
    input_(input),
    eventSkipperByID_(EventSkipperByID::create(pset).release()),
    duplicateChecker_(new DuplicateChecker(pset)),
    initialNumberOfEventsToSkip_(pset.getUntrackedParameter<unsigned int>("skipEvents")),
    noEventSort_(pset.getUntrackedParameter<bool>("noEventSort")),
    treeCacheSize_(noEventSort_ ? pset.getUntrackedParameter<unsigned int>("cacheSize") : 0U)
  {
  }

  std::vector<FileCatalogItem> const&
  RootInputFileQueue::fileCatalogItems() const {
    return catalog_.fileCatalogItems();
  }

  std::shared_ptr<ProductRegistry const>
  RootInputFileQueue::fileProductRegistry() const {
    assert(rootFile());
    return rootFile()->productRegistry();
  }

  std::shared_ptr<BranchIDListHelper const>
  RootInputFileQueue::fileBranchIDListHelper() const {
    assert(rootFile());
    return rootFile()->branchIDListHelper();
  }

  RootInputFileQueue::~RootInputFileQueue() {
  }

  bool
  RootInputFileQueue::containedInCurrentFile(RunNumber_t run, LuminosityBlockNumber_t lumi, EventNumber_t event) const {
    if(!rootFile()) return false;
    return rootFile()->containsItem(run, lumi, event);
  }

  bool
  RootInputFileQueue::skipToItem(RunNumber_t run, LuminosityBlockNumber_t lumi, EventNumber_t event, size_t fileNameHash, bool currentFileFirst) {
    // Attempt to find item in currently open input file.
    bool found = currentFileFirst && rootFile() && rootFile()->setEntryAtItem(run, lumi, event);

    // As with RootInputFileSequence, we could maintain a hashmap of previously-read files and search
    // those files; doesn't seem like it is necessary in this case?
    return found;
  }

/*
  void
  RootInputFileQueue::initTheFile(bool skipBadFiles,
                                  bool deleteIndexIntoFile,
                                  InputSource* input, 
                                  char const* inputTypeName,
                                  InputType inputType) {
    // We are really going to close the open file.

    closeFile_();

    if(noMoreFiles()) {
      // No files specified
      return;
    }

    // Check if the logical file name was found.
    if(fileName().empty()) {
      // LFN not found in catalog.
      InputFile::reportSkippedFile(fileName(), logicalFileName());
      if(!skipBadFiles) {
        throw cms::Exception("LogicalFileNameNotFound", "RootInputFileQueue::initTheFile()\n")
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
      std::unique_ptr<InputSource::FileOpenSentry> sentry(input ? std::make_unique<InputSource::FileOpenSentry>(*input, lfn_, usedFallback_) : nullptr);
      std::unique_ptr<char[]> name(gSystem->ExpandPathName(fileName().c_str()));;
      filePtr = std::make_shared<InputFile>(name.get(), "  Initiating request to open file ", inputType);
    }
    catch (cms::Exception const& e) {
      if(!skipBadFiles) {
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
          ex.addContext("Calling RootInputFileQueue::initTheFile()");
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
        std::unique_ptr<InputSource::FileOpenSentry> sentry(input ? std::make_unique<InputSource::FileOpenSentry>(*input, lfn_, usedFallback_) : nullptr);
        std::unique_ptr<char[]> fallbackFullName(gSystem->ExpandPathName(fallbackFileName().c_str()));
        filePtr.reset(new InputFile(fallbackFullName.get(), "  Fallback request to file ", inputType));
      }
      catch (cms::Exception const& e) {
        if(!skipBadFiles) {
          InputFile::reportSkippedFile(fileName(), logicalFileName());
          Exception ex(errors::FallbackFileOpenError, "", e);
          ex.addContext("Calling RootInputFileQueue::initTheFile()");
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
      if(input) {
        rootFile_->setSignals(&(input->preEventReadFromSourceSignal_), &(input->postEventReadFromSourceSignal_));
      }
      assert(rootFile_);
      rootFile_->reportOpened(inputTypeName);
    } else {
      InputFile::reportSkippedFile(fileName(), logicalFileName());
      if(!skipBadFiles) {
        throw Exception(errors::FileOpenError) <<
           "RootInputFileQueue::initTheFile(): Input file " << fileName() << " was not found or could not be opened.\n";
      }
      LogWarning("") << "Input file: " << fileName() << " was not found or could not be opened, and will be skipped.\n";
    }
  }

  std::unique_ptr<FileBlock>
  RootInputFileQueue::readFile_() {
    if (!filesProcessed) {
      filesProcessed++;
      // The first input file has already been opened.
      if (!rootFile()) {
        initFile(input_.skipBadFiles());
      }
    } else {
      if (!nextFile()) {
        assert(0);
      }
    }
    if (!rootFile()) {
      return std::make_unique<FileBlock>();
    }
    return rootFile()->createFileBlock();
  }
*/
/*
  InputSource::ItemType
  RootInputFileQueue::getNextItemType(RunNumber_t& run, LuminosityBlockNumber_t& lumi, EventNumber_t& event) {
    if(noMoreFiles()) {
      return InputSource::IsStop;
    } 
    if (!filesProcessed) {
      return InputSource::IsFile;
    } 
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
    } 
    if(atLastFile()) {
      return InputSource::IsStop;
    } 
    return InputSource::IsFile;
  }
*/
  bool RootInputFileQueue::nextFile() {
    if(!noMoreFiles()) {
      // TODO: Contact remote queue and ask for a file.
      return false;
    }
    if(noMoreFiles()) {
      return false;
    } 

    initFile(input_.skipBadFiles());
    
    if(rootFile()) {
      // make sure the new product registry is compatible with the main one.
      // For now, default to 'permissive mode' for branches matching.
      std::string mergeInfo = input_.productRegistryUpdate().merge(*rootFile()->productRegistry(),
                                                                   fileName(),
                                                                   BranchDescription::Permissive);
      if(!mergeInfo.empty()) {                                     
        throw Exception(errors::MismatchedInputFiles,"RootInputFileQueue::nextFile()") << mergeInfo;
      } 
    } 
    return true;
  }

  void
  RootInputFileQueue::fillDescription(ParameterSetDescription & desc) {
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

    // TODO: add AMQP-specific elements here.
  }

  // Advance "offset" events.  Offset can be positive or negative (or zero).
  bool
  RootInputFileQueue::skipEvents(int offset) {
    assert(rootFile());
    while(offset != 0) {
      bool atEnd = rootFile()->skipEvents(offset);
      if((offset > 0 || atEnd) && !nextFile()) {
        return false;
      }
      if (offset < 0) {
        return false;
      }
    }
    return true;
  }


  void
  RootInputFileQueue::setIndexIntoFile(size_t index) {
   indexesIntoFiles_[index] = rootFile()->indexIntoFileSharedPtr();
  }

  void
  RootInputFileQueue::initFile_(bool skipBadFiles) {
    // If we are not duplicate checking across files and we are not using random access to find events,
    // then we can delete the IndexIntoFile for the file we are closing.
    // If we can't delete all of it, then we can delete the parts we do not need.
    bool deleteIndexIntoFile = !(duplicateChecker_ && duplicateChecker_->checkingAllFiles() && !duplicateChecker_->checkDisabled());
    initTheFile(skipBadFiles, deleteIndexIntoFile, &input_, "primaryFiles", InputType::Primary);
  }   

}

