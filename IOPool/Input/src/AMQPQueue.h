#ifndef IOPool_Input_AMQPQueue_h
#define IOPool_Input_AMQPQueue_h

#include <string>
#include <vector>

#include "FileQueue.h"

namespace edm {

  class ParameterSet;

  class AMQPQueue final : public FileQueue {
  public:
    explicit AMQPQueue(ParameterSet const& pset);
    virtual ~AMQPQueue();

    virtual bool next(std::string &filename) override;

  private:
    std::vector<std::string> filenames_;
    std::vector<std::string>::const_iterator iter_;
  };

}

#endif  // IOPool_Input_AMQPQueue_h
