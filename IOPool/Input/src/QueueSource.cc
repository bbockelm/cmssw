/*----------------------------------------------------------------------
----------------------------------------------------------------------*/
#include "QueueSource.h"
#include "InputFile.h"
#include "RootInputFileQueue.h"
#include "RunHelper.h"
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
#include "FWCore/ParameterSet/interface/ConfigurationDescriptions.h"
#include "FWCore/ParameterSet/interface/ParameterSetDescription.h"
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
    catalog_(pset.getUntrackedParameter<std::vector<std::string> >("fileNames"),
      pset.getUntrackedParameter<std::string>("overrideCatalog", std::string())),
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
    primaryFileSequence_(new RootInputFileQueue(pset, *this, catalog_))
  {
    auto resources = SharedResourcesRegistry::instance()->createAcquirerForSourceDelayedReader();
    resourceSharedWithDelayedReaderPtr_ = std::make_unique<SharedResourcesAcquirer>(std::move(resources.first));
    mutexSharedWithDelayedReader_ = resources.second;
  }

  QueueSource::~QueueSource() {}

  void
  QueueSource::endJob() {
    primaryFileSequence_->endJob();
    InputFile::reportReadBranches();
  }

  std::unique_ptr<FileBlock>
  QueueSource::readFile_() {
    std::unique_ptr<FileBlock> fb = primaryFileSequence_->readFile_();
    return fb;
  }

  void QueueSource::closeFile_() {
    primaryFileSequence_->closeFile_();
  }

  std::shared_ptr<RunAuxiliary>
  QueueSource::readRunAuxiliary_() {
    return primaryFileSequence_->readRunAuxiliary_();
  }

  std::shared_ptr<LuminosityBlockAuxiliary>
  QueueSource::readLuminosityBlockAuxiliary_() {
    return primaryFileSequence_->readLuminosityBlockAuxiliary_();
  }

  void
  QueueSource::readRun_(RunPrincipal& runPrincipal) {
    primaryFileSequence_->readRun_(runPrincipal);
  }

  void
  QueueSource::readLuminosityBlock_(LuminosityBlockPrincipal& lumiPrincipal) {
    primaryFileSequence_->readLuminosityBlock_(lumiPrincipal);
  }

  void
  QueueSource::readEvent_(EventPrincipal& eventPrincipal) {
    primaryFileSequence_->readEvent(eventPrincipal);
  }

  bool
  QueueSource::readIt(EventID const& id, EventPrincipal& eventPrincipal, StreamContext& streamContext) {
    bool found = primaryFileSequence_->skipToItem(id.run(), id.luminosityBlock(), id.event());
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
    InputSource::ItemType itemType = primaryFileSequence_->getNextItemType(run, lumi, event);
    return runHelper_->nextItemType(state(), itemType);
  }

  void
  QueueSource::preForkReleaseResources() {
    primaryFileSequence_->closeFile_();
  }

  std::pair<SharedResourcesAcquirer*,std::recursive_mutex*>
  QueueSource::resourceSharedWithDelayedReader_() {
    return std::make_pair(resourceSharedWithDelayedReaderPtr_.get(), mutexSharedWithDelayedReader_.get());
  }

  // Advance "offset" events.  Offset can be positive or negative (or zero).
  void
  QueueSource::skip(int offset) {
    primaryFileSequence_->skipEvents(offset);
  }

  bool
  QueueSource::goToEvent_(EventID const& eventID) {
    return primaryFileSequence_->goToEvent(eventID);
  }

  void
  QueueSource::fillDescriptions(ConfigurationDescriptions & descriptions) {

    ParameterSetDescription desc;

    std::vector<std::string> defaultStrings;
    desc.setComment("Reads EDM/ROOT files determined by a remote queue.");
    desc.addUntracked<std::vector<std::string> >("contact")
        ->setComment("Contact URI for remote queue.");
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
    ProductSelectorRules::fillDescription(desc, "inputCommands");
    InputSource::fillDescription(desc);
    RootInputFileQueue::fillDescription(desc);
    RunHelperBase::fillDescription(desc);

    descriptions.add("source", desc);
  }

  bool
  QueueSource::randomAccess_() const {
    return false;
  }

}
