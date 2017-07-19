#ifndef IOPool_Input_QueueSource_h
#define IOPool_Input_QueueSource_h

/*----------------------------------------------------------------------

QueueSource: This is an InputSource that processes ROOT files, but whose
activities are governed by a remote queue.

----------------------------------------------------------------------*/

#include "DataFormats/Provenance/interface/BranchType.h"
#include "FWCore/Catalog/interface/InputFileCatalog.h"
#include "FWCore/Framework/interface/Frameworkfwd.h"
#include "FWCore/Framework/interface/ProcessingController.h"
#include "FWCore/Framework/interface/ProductSelectorRules.h"
#include "FWCore/Framework/interface/InputSource.h"
#include "FWCore/Utilities/interface/propagate_const.h"
#include "IOPool/Common/interface/RootServiceChecker.h"

#include <array>
#include <memory>
#include <string>
#include <vector>

namespace edm {

  class ConfigurationDescriptions;
  class DuplicateChecker;
  class EventSkipperByID;
  class FileCatalogItem;
  class IndexIntoFile;
  class InputFile;
  class RootFile;
  class RunHelperBase;
  class FileQueue;

  class QueueSource : public InputSource {
  public:
    explicit QueueSource(ParameterSet const& pset, InputSourceDescription const& desc);
    virtual ~QueueSource();
    using InputSource::processHistoryRegistryForUpdate;
    using InputSource::productRegistryUpdate;

    // const accessors
    bool dropDescendants() const {return dropDescendants_;}
    bool bypassVersionCheck() const {return bypassVersionCheck_;}
    bool labelRawDataLikeMC() const {return labelRawDataLikeMC_;}
    unsigned int nStreams() const {return nStreams_;}
    int treeMaxVirtualSize() const {return treeMaxVirtualSize_;}
    ProductSelectorRules const& productSelectorRules() const {return productSelectorRules_;}
    RunHelperBase* runHelper() {return runHelper_.get();}

    static void fillDescriptions(ConfigurationDescriptions& descriptions);

  private:
    typedef std::shared_ptr<RootFile> RootFileSharedPtr;

    std::string const& fileName() const {return fileCatalogItem_.fileName();}
    std::string const& logicalFileName() const {return fileCatalogItem_.logicalFileName();}
    std::string const& fallbackFileName() const {return fileCatalogItem_.fallbackFileName();}
    std::string const& lfn() const {return lfn_;}
    size_t lfnHash() const {return lfnHash_;}
    bool usedFallback() const {return usedFallback_;}
    size_t sequenceNumberOfFile() const {assert(fileSequenceNum >= 0); return fileSequenceNum;}
    bool atFirstFile() const {return fileSequenceNum == 0;}
    bool atLastFile() const {return (lastFileSequenceNum >= 0) && (fileSequenceNum == lastFileSequenceNum) ;}
    bool noMoreFiles() const {return (lastFileSequenceNum == -1) || ((lastFileSequenceNum != -2) && (fileSequenceNum > lastFileSequenceNum));}
    bool noFiles() const {return lastFileSequenceNum == -1;}

    virtual void readEvent_(EventPrincipal& eventPrincipal) override;
    virtual std::shared_ptr<LuminosityBlockAuxiliary> readLuminosityBlockAuxiliary_() override;
    virtual void readLuminosityBlock_(LuminosityBlockPrincipal& lumiPrincipal) override;
    virtual std::shared_ptr<RunAuxiliary> readRunAuxiliary_() override;
    virtual void readRun_(RunPrincipal& runPrincipal) override;
    virtual std::unique_ptr<FileBlock> readFile_() override;
    virtual void closeFile_() override;
    virtual void endJob() override;
    virtual ItemType getNextItemType() override;
    ItemType getNextItemTypeFromFile(RunNumber_t& run, LuminosityBlockNumber_t& lumi, EventNumber_t& event);
    virtual bool readIt(EventID const& id, EventPrincipal& eventPrincipal, StreamContext& streamContext) override;
    virtual void skip(int offset) override;
    virtual bool goToEvent_(EventID const& eventID) override;
    virtual void preForkReleaseResources() override;
    virtual bool randomAccess_() const override;

    void jobSuccess();
    void jobFailure();

    void initFile();
    bool skipToItem(RunNumber_t run, LuminosityBlockNumber_t lumi, EventNumber_t event);
    std::vector<std::shared_ptr<IndexIntoFile> > const& indexesIntoFiles() const {return indexesIntoFiles_;}
    void setIndexIntoFile(size_t index);
    RootFileSharedPtr makeRootFile(std::shared_ptr<InputFile> filePtr);
    bool nextFile();

    std::shared_ptr<EventSkipperByID const> eventSkipperByID() const {return get_underlying_safe(eventSkipperByID_);}
    std::shared_ptr<EventSkipperByID>& eventSkipperByID() {return get_underlying_safe(eventSkipperByID_);}
    std::shared_ptr<DuplicateChecker const> duplicateChecker() const {return get_underlying_safe(duplicateChecker_);}
    std::shared_ptr<DuplicateChecker>& duplicateChecker() {return get_underlying_safe(duplicateChecker_);}
    std::shared_ptr<RootFile const> rootFile() const {return get_underlying_safe(rootFile_);}
    std::shared_ptr<RootFile>& rootFile() {return get_underlying_safe(rootFile_);}

    std::pair<SharedResourcesAcquirer*,std::recursive_mutex*> resourceSharedWithDelayedReader_() override;

    RootServiceChecker rootServiceChecker_;
    std::array<std::vector<BranchID>, NumBranchTypes>  branchIDsToReplace_;

    unsigned int nStreams_;
    bool skipBadFiles_;
    bool bypassVersionCheck_;
    int const treeMaxVirtualSize_;
    ProductSelectorRules productSelectorRules_;
    bool dropDescendants_;
    bool labelRawDataLikeMC_;
    
    edm::propagate_const<std::unique_ptr<RunHelperBase>> runHelper_;
    std::unique_ptr<SharedResourcesAcquirer> resourceSharedWithDelayedReaderPtr_; // We do not use propagate_const because the acquirer is itself mutable.
    std::shared_ptr<std::recursive_mutex> mutexSharedWithDelayedReader_;

    std::vector<ProcessHistoryID> orderedProcessHistoryIDs_;
    edm::propagate_const<std::shared_ptr<EventSkipperByID>> eventSkipperByID_;
    edm::propagate_const<std::shared_ptr<DuplicateChecker>> duplicateChecker_;
    int initialNumberOfEventsToSkip_;
    bool noEventSort_;
    unsigned int treeCacheSize_;

    std::string overrideCatalogLocation_{};
    std::string lfn_{"unknown"};
    size_t lfnHash_{0U};
    bool usedFallback_{false};
    bool readFileInvokedOnce_{false};
    FileCatalogItem fileCatalogItem_;
    // lastFileSequenceNum is the sequence number of the last file to process.
    // If we haven't been told what the last number is yet, then we keep this set to -2.
    // The special value of -1 indicates there are no files to process.
    ssize_t lastFileSequenceNum {-2};
    ssize_t fileSequenceNum {-1};
    edm::propagate_const<RootFileSharedPtr> rootFile_;
    std::vector<std::shared_ptr<IndexIntoFile> > indexesIntoFiles_;

    // A list of files that are pending acknowledgement.  These should trigger an ack
    // to the remote queue when the source stops (or an output file is safely staged out).
    std::vector<std::string> filesToAck_;

    std::unique_ptr<FileQueue> fileQueue_;
  }; // class QueueSource
}
#endif
