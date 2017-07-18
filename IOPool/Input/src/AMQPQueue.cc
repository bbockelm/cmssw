
#include "AMQPQueue.h"

#include "FWCore/ParameterSet/interface/ParameterSetDescription.h"

namespace edm {

  AMQPQueue::AMQPQueue(ParameterSet const& pset) :
    filenames_(pset.getUntrackedParameter<std::vector<std::string>>("fileNames")),
    iter_(filenames_.begin())
  {}

  AMQPQueue::~AMQPQueue() {}

  bool
  AMQPQueue::next(std::string &name)
  {
    if (iter_ == filenames_.end()) {return false;}
    name = *iter_;
    iter_++;
    return true;
  }

}

