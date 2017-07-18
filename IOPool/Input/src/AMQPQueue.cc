
#include "AMQPQueue.h"

#include "amqp_tcp_socket.h"
#include <amqp.h>
#include <amqp_framing.h>

#include "FWCore/ParameterSet/interface/ParameterSetDescription.h"
#include "FWCore/Utilities/interface/EDMException.h"

namespace {


  void
  throwAMQPError(amqp_rpc_reply_t reply, std::string const& context, std::string const& msg)
  {
    edm::Exception ex(edm::errors::OtherCMS);
    ex << msg;
    ex.addContext("Calling " + context);
    std::stringstream ss;
    switch (reply.reply_type) {
    case AMQP_RESPONSE_NORMAL:
      ex.addAdditionalInfo("Logic error: exception thrown for successful RPC!");
      break;
    case AMQP_RESPONSE_NONE:
      ex.addAdditionalInfo("Internal error: library failed to return response!");
      break;
    case AMQP_RESPONSE_LIBRARY_EXCEPTION:
      ss << "Library exception: " << amqp_error_string2(reply.library_error);
      ex.addAdditionalInfo(ss.str());
      break;
    case AMQP_RESPONSE_SERVER_EXCEPTION:
      switch (reply.reply.id) {
      case AMQP_CONNECTION_CLOSE_METHOD: {
        amqp_connection_close_t *m = (amqp_connection_close_t *) reply.reply.decoded;
        std::string message((char*)m->reply_text.bytes, m->reply_text.len);
        ss << "Server connection exception code " << m->reply_code << "; message: " << message;
        ex.addAdditionalInfo(ss.str());
        break;
      }
      case AMQP_CHANNEL_CLOSE_METHOD: {
        amqp_channel_close_t *m = (amqp_channel_close_t *) reply.reply.decoded;
        std::string message((char*)m->reply_text.bytes, m->reply_text.len);
        ss << "Server channel exception code " << m->reply_code << "; message: " << message;
        ex.addAdditionalInfo(ss.str());
        break;
      }
      }
    }
    throw ex;
  }


  void
  throwOnAMQPError(amqp_rpc_reply_t reply, std::string const &context, std::string const &msg)
  {
    if (reply.reply_type != AMQP_RESPONSE_NORMAL) {
      throwAMQPError(reply, context, msg);
    }
  }
}

namespace edm {

  namespace internal {

    class AMQPState {
      friend class edm::AMQPQueue;

    public:
      ~AMQPState()
      {
        if (conn_) {
          if (channelOpened_) {
            amqp_channel_close(conn_, 1, AMQP_REPLY_SUCCESS);  // TODO: check error code.
          }
          if (socketConnected_) {
            amqp_connection_close(conn_, AMQP_REPLY_SUCCESS);  // TODO: check error code.
          }
          amqp_destroy_connection(conn_);  // TODO: check error code.
        }
        if (queuename_.bytes) amqp_bytes_free(queuename_);
      }

    private:
      AMQPState(){}

      void
      throwOnRPCError(std::string const &context, std::string const& msg)
      {
        auto reply = amqp_get_rpc_reply(conn_);
        throwOnAMQPError(reply, context, msg);
      }

      amqp_connection_state_t conn_{nullptr};
      amqp_bytes_t queuename_{0, nullptr};
      bool socketConnected_{false};
      bool channelOpened_{false};

    };

  }

  AMQPQueue::AMQPQueue(ParameterSet const& pset) :
    hostname_(pset.getUntrackedParameter<std::string>("hostname")),
    queue_(pset.getUntrackedParameter<std::string>("queue")),
    filenames_(pset.getUntrackedParameter<std::vector<std::string>>("fileNames")),
    iter_(filenames_.begin()),
    state_(new internal::AMQPState())
  {
    setupConnection();
    setupChannel();
    setupConsumeQueue();
  }

  AMQPQueue::~AMQPQueue() {}

  bool
  AMQPQueue::next(std::string &name)
  {
    amqp_rpc_reply_t ret;
    amqp_envelope_t envelope;
    amqp_frame_t frame;

    // amqp_maybe_release_buffers(conn);  ?
    ret = amqp_consume_message(state_->conn_, &envelope, nullptr, 0);

    if (AMQP_RESPONSE_NORMAL != ret.reply_type) {
      if (AMQP_RESPONSE_LIBRARY_EXCEPTION == ret.reply_type &&
          AMQP_STATUS_UNEXPECTED_STATE == ret.library_error) {


        if (AMQP_STATUS_OK != amqp_simple_wait_frame(state_->conn_, &frame)) {
        // TODO: do something?  Throw an exception?
          edm::Exception ex(edm::errors::OtherCMS);
          ex << "Failed to wait on the next frame.";
          ex.addContext("AMQPQueue::next()");
          throw ex;
        }
        if (AMQP_FRAME_METHOD == frame.frame_type) {
          switch (frame.payload.method.id) {
          case AMQP_CHANNEL_CLOSE_METHOD: {
            edm::Exception ex(edm::errors::OtherCMS);
            ex << "Remote channel was unexpectedly closed.";
            ex.addContext("AMQPQueue::next()");
            throw ex;
          }
          case AMQP_CONNECTION_CLOSE_METHOD: {
            edm::Exception ex(edm::errors::OtherCMS);
            ex << "Remote connection was unexpectedly closed.";
            ex.addContext("AMQPQueue::next()");
            throw ex;
          }
          default: {
            edm::Exception ex(edm::errors::OtherCMS);
            ex << "An unexpected AMQP method was received " << frame.payload.method.id;
            ex.addContext("AMQPQueue::next()");
            throw ex;
          }
          };
        } else {
          edm::Exception ex(edm::errors::OtherCMS);
          ex << "An unexpected AMQP frame (without a header) was received.";
          ex.addContext("AMQPQueue::next()");
          throw ex;
        }
      }
    } else {  // ret.reply_type == AMQP_RESPONSE_NORMAL
      // TODO: process envelope.
      amqp_destroy_envelope(&envelope);
      edm::Exception ex(edm::errors::UnimplementedFeature);
      ex << "Not implemented error.";
      ex.addContext("AMQPQueue::next()");
      throw ex;
    }

    if (iter_ == filenames_.end()) {return false;}
    name = *iter_;
    iter_++;
    return true;
  }

  void
  AMQPQueue::fillDescription(ParameterSetDescription& desc)
  {
    desc.addUntracked<std::string>("hostname")
      ->setComment("Contact hostname for message broker.");
    desc.addUntracked<std::string>("queue")
      ->setComment("Remote queue for subscription.");
    desc.addUntracked<std::vector<std::string>>("fileNames")
      ->setComment("(Stub) additional filenames to process.");
  }

  void
  AMQPQueue::setupConnection()
  {
    state_->conn_ = amqp_new_connection();
    if (!state_->conn_) {
      edm::Exception ex(edm::errors::BadAlloc);
      ex << "Failed to create new AMQP connection object.";
      ex.addContext("Calling AMQPQueue::setupConnection()");
      throw ex;
    }

    amqp_socket_t *socket = amqp_tcp_socket_new(state_->conn_);
    if (!socket) {
      edm::Exception ex(edm::errors::BadAlloc);
      ex << "Failed to create a new TCP socket.";
      ex.addContext("Calling AMQPQueue::setupConnection()");
      throw ex;
    }

    int status = amqp_socket_open(socket, hostname_.c_str(), 5672);
    if (status) {
      edm::Exception ex(edm::errors::OtherCMS);
      ex << "Failed to connect to AMQP broker via TCP.";
      ex.addContext("Calling AMQPQueue::setupConnection()");
      throw ex;
    }
    state_->socketConnected_ = true;

    throwOnAMQPError(
      amqp_login(state_->conn_, "/", 0, 131072, 0, AMQP_SASL_METHOD_PLAIN, "guest", "guest"),
      "AMQP::setupConnection()", "Failed to login to AMQP broker."
    );
  }

  void
  AMQPQueue::setupChannel()
  {
    amqp_channel_open(state_->conn_, 1),
    state_->throwOnRPCError("AMQPQueue::setupChannel()", "Failed to open channel to AMQP broker.");
    state_->channelOpened_ = true;

    // TODO: should `r` here be free'd afterward?
    {
      amqp_queue_declare_ok_t *r = amqp_queue_declare(state_->conn_, 1, amqp_empty_bytes, 0, 0, 0, 1,
                                   amqp_empty_table);
      state_->throwOnRPCError("AMQPQueue::setupChannel()", "Failed to declare queue to AMQP broker.");

      amqp_bytes_t queuename = amqp_bytes_malloc_dup(r->queue);
      if (queuename.bytes == nullptr) {
        edm::Exception ex(edm::errors::BadAlloc);
        ex << "Failed to allocate a new queue name.";
        ex.addContext("Calling AMQP::setupChannel()");
        throw ex;
      }
      state_->queuename_ = queuename;
    }

    const std::string exchange = "amq.direct";
    amqp_queue_bind(state_->conn_, 1, state_->queuename_, amqp_cstring_bytes(exchange.c_str()),
                    amqp_cstring_bytes(queue_.c_str()), amqp_empty_table);
    state_->throwOnRPCError("AMQPQueue::setupChannel()", "Failed to bind to queue.");
  }

  void
  AMQPQueue::setupConsumeQueue()
  {
    amqp_basic_consume(state_->conn_, 1, state_->queuename_, amqp_empty_bytes, 0, 1, 0,
                       amqp_empty_table);
    state_->throwOnRPCError("AMQPQueue::setupChannel()", "Failed to setup basic consume.");
  }

}

