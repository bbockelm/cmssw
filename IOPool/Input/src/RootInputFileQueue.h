#ifndef IOPool_Input_RootInputFileQueue_h
#define IOPool_Input_RootInputFileQueue_h

/*----------------------------------------------------------------------

RootInputFileQueue: An iterator of input files, based on the contents of
a remote queue.

----------------------------------------------------------------------*/

#include "InputFile.h"
#include "FWCore/Framework/interface/Frameworkfwd.h"
#include "FWCore/Catalog/interface/InputFileCatalog.h"
#include "FWCore/Utilities/interface/InputType.h"
#include "FWCore/Utilities/interface/get_underlying_safe.h"

#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

namespace edm {

  class FileCatalogItem;
  class IndexIntoFile;
  class InputFileCatalog;
  class ParameterSetDescription;
  class RootFile;
  class QueueSource;

  class RootInputFileQueue {
  public:
    explicit RootInputFileQueue(ParameterSet const& pset,
                                QueueSource &input,
                                InputFileCatalog const& catalog);
    virtual ~RootInputFileQueue();

    RootInputFileQueue(RootInputFileQueue const&) = delete; // Disallow copying and moving
    RootInputFileQueue& operator=(RootInputFileQueue const&) = delete; // Disallow copying and moving

    bool containedInCurrentFile(RunNumber_t run, LuminosityBlockNumber_t lumi, EventNumber_t event) const;
    void readEvent(EventPrincipal& cache);
    std::shared_ptr<LuminosityBlockAuxiliary> readLuminosityBlockAuxiliary_();
    void readLuminosityBlock_(LuminosityBlockPrincipal& lumiPrincipal);
    std::shared_ptr<RunAuxiliary> readRunAuxiliary_();
    void readRun_(RunPrincipal& runPrincipal);
    bool skipToItem(RunNumber_t run, LuminosityBlockNumber_t lumi, EventNumber_t event, size_t fileNameHash = 0U, bool currentFileFirst = true);
    std::shared_ptr<ProductRegistry const> fileProductRegistry() const;
    std::shared_ptr<BranchIDListHelper const> fileBranchIDListHelper() const;
    std::unique_ptr<FileBlock> readFile_();
    void endJob();
    virtual void initFile_(bool skipBadFiles);
    virtual void closeFile_();
    bool skipEvents(int offset);
    bool goToEvent(EventID const& eventID);
    InputSource::ItemType getNextItemType(RunNumber_t& run, LuminosityBlockNumber_t& lumi, EventNumber_t& event);
    static void fillDescription(ParameterSetDescription & desc);

  protected:
    typedef std::shared_ptr<RootFile> RootFileSharedPtr;
    void initFile(bool skipBadFiles) {initFile_(skipBadFiles);}
    void initTheFile(bool skipBadFiles, bool deleteIndexIntoFile, InputSource* input, char const* inputTypeName, InputType inputType);
    bool skipToItemInNewFile(RunNumber_t run, LuminosityBlockNumber_t lumi, EventNumber_t event);
    bool skipToItemInNewFile(RunNumber_t run, LuminosityBlockNumber_t lumi, EventNumber_t event, size_t fileNameHash);
    virtual RootFileSharedPtr makeRootFile(std::shared_ptr<InputFile> filePtr);

    bool nextFile();

    bool atFirstFile() const {return filesProcessed == 0;}
    bool atLastFile() const {return gotLastFile;}
    bool noMoreFiles() const {return gotLastFile;}
    bool noFiles() const {return (filesProcessed == 0) && gotLastFile;}

    void setAtNextFile();
    void setAtPreviousFile();

    std::string const& fileName() const {return fileIter_.fileName();}
    std::string const& logicalFileName() const {return fileIter_.logicalFileName();}
    std::string const& fallbackFileName() const {return fileIter_.fallbackFileName();}
    std::string const& lfn() const {return lfn_;}
    std::vector<FileCatalogItem> const& fileCatalogItems() const;

    size_t lfnHash() const {return lfnHash_;}
    bool usedFallback() const {return usedFallback_;}

    std::shared_ptr<RootFile const> rootFile() const {return get_underlying_safe(rootFile_);}
    std::shared_ptr<RootFile>& rootFile() {return get_underlying_safe(rootFile_);}

  private:
    InputFileCatalog const& catalog_;
    QueueSource& input_;
    std::string lfn_{"unknown"};
    size_t lfnHash_{0U};
    bool usedFallback_{false};
    FileCatalogItem fileIter_;
    bool gotLastFile {false};
    size_t filesProcessed {0};
    edm::propagate_const<RootFileSharedPtr> rootFile_;

  private:


  }; // class RootInputFileSequence
}
#endif
