#ifndef IOPool_Input_AMQPQueue_h
#define IOPool_Input_AMQPQueue_h

#include <string>
#include <vector>
#include <memory>

#include "FileQueue.h"

namespace edm {

  namespace internal {
    class AMQPState;
  }

  class ParameterSet;
  class ParameterSetDescription;

  class AMQPQueue final : public FileQueue {
  public:
    explicit AMQPQueue(ParameterSet const& pset);
    virtual ~AMQPQueue();

    virtual bool next(std::string &filename) override;

    static void fillDescription(ParameterSetDescription& descriptions);

  private:
    void setupConnection();
    void setupChannel();
    void setupConsumeQueue();

    std::string hostname_;
    static const uint32_t port_{5672};
    const uint32_t timeout_sec_{2};
    std::string queue_;

    // Opaque structure only defined in AMQPQueue.cc; meant to keep amqp-specific headers
    // from leaking out to other compilation units.
    std::unique_ptr<internal::AMQPState> state_;
  };

}

#endif  // IOPool_Input_AMQPQueue_h
