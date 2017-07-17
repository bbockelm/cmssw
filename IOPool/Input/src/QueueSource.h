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
  class FileCatalogItem;
  class RootInputFileQueue;
  class RunHelperBase;

  class QueueSource : public InputSource {
  public:
    explicit QueueSource(ParameterSet const& pset, InputSourceDescription const& desc);
    virtual ~QueueSource();
    using InputSource::processHistoryRegistryForUpdate;
    using InputSource::productRegistryUpdate;

    // const accessors
    bool skipBadFiles() const {return skipBadFiles_;}
    bool dropDescendants() const {return dropDescendants_;}
    bool bypassVersionCheck() const {return bypassVersionCheck_;}
    bool labelRawDataLikeMC() const {return labelRawDataLikeMC_;}
    unsigned int nStreams() const {return nStreams_;}
    int treeMaxVirtualSize() const {return treeMaxVirtualSize_;}
    ProductSelectorRules const& productSelectorRules() const {return productSelectorRules_;}
    RunHelperBase* runHelper() {return runHelper_.get();}

    static void fillDescriptions(ConfigurationDescriptions& descriptions);

  private:
    virtual void readEvent_(EventPrincipal& eventPrincipal) override;
    virtual std::shared_ptr<LuminosityBlockAuxiliary> readLuminosityBlockAuxiliary_() override;
    virtual void readLuminosityBlock_(LuminosityBlockPrincipal& lumiPrincipal) override;
    virtual std::shared_ptr<RunAuxiliary> readRunAuxiliary_() override;
    virtual void readRun_(RunPrincipal& runPrincipal) override;
    virtual std::unique_ptr<FileBlock> readFile_() override;
    virtual void closeFile_() override;
    virtual void endJob() override;
    virtual ItemType getNextItemType() override;
    virtual bool readIt(EventID const& id, EventPrincipal& eventPrincipal, StreamContext& streamContext) override;
    virtual void skip(int offset) override;
    virtual bool goToEvent_(EventID const& eventID) override;
    virtual void preForkReleaseResources() override;
    virtual bool randomAccess_() const override;

    std::pair<SharedResourcesAcquirer*,std::recursive_mutex*> resourceSharedWithDelayedReader_() override;
    
    RootServiceChecker rootServiceChecker_;
    InputFileCatalog catalog_;
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
    edm::propagate_const<std::unique_ptr<RootInputFileQueue>> primaryFileSequence_;
  }; // class QueueSource
}
#endif
