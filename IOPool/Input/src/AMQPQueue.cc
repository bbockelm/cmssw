
#include "AMQPQueue.h"

#include <iostream>

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
          conn_ = nullptr;
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
    timeout_sec_(pset.getUntrackedParameter<unsigned>("timeout")),
    queue_(pset.getUntrackedParameter<std::string>("queue")),
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

    struct timeval timeout{.tv_sec = timeout_sec_, .tv_usec = 0};

    amqp_maybe_release_buffers(state_->conn_);
    ret = amqp_consume_message(state_->conn_, &envelope, &timeout, 0);

    if (AMQP_RESPONSE_LIBRARY_EXCEPTION == ret.reply_type) {
      if (AMQP_STATUS_UNEXPECTED_STATE == ret.library_error) {

        if (AMQP_STATUS_OK != amqp_simple_wait_frame(state_->conn_, &frame)) {
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
      } else if (AMQP_STATUS_TIMEOUT == ret.library_error) {
        // Nothing left for this process to do: time to shut down cleanly.
        std::cout << "Timeout occurred waiting for more files to process.  Shutting down.\n";
        return false;
      } else {
        throwAMQPError(ret, "AMQPQueue::next()", "Response from broker.");
      }
    } else if (ret.reply_type == AMQP_RESPONSE_NORMAL) {

      std::string exchange((char*)envelope.exchange.bytes, envelope.exchange.len);
      std::string routing_key((char*)envelope.routing_key.bytes, envelope.routing_key.len);
      //std::cout << "Delivery " << envelope.delivery_tag << ", exchange "
      //  << exchange << ", routing key " << routing_key << "\n";
      // TODO: process / check the Content-type header?
      std::string message((char*)envelope.message.body.bytes, envelope.message.body.len);
      std::cout << "AMQP host returned file to process: " << message << std::endl;
      fileToTag_.emplace(message, envelope.delivery_tag);

      amqp_destroy_envelope(&envelope);
      name = message;
    } else {
      throwAMQPError(ret, "AMQPQueue::next()", "Retrieving file to process.");
    }
    return true;
  }

  void
  AMQPQueue::fillDescription(ParameterSetDescription& desc)
  {
    desc.addUntracked<std::string>("hostname")
      ->setComment("Contact hostname for message broker.");
    desc.addUntracked<std::string>("queue")
      ->setComment("Remote queue for subscription.");
    desc.addUntracked<unsigned>("timeout", 2)
      ->setComment("Timeout to wait for new files, in seconds.");
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
      amqp_queue_declare_ok_t *r = amqp_queue_declare(state_->conn_, 1,
          amqp_cstring_bytes(queue_.c_str()),
          0, // passive
          0, // durable
          0, // exclusive
          0, // auto_delete
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
    amqp_basic_consume(state_->conn_, 1,
                       state_->queuename_,
                       amqp_empty_bytes, // consumer_tag
                       0, // no_local
                       0, // no_ack
                       0, // exclusive
                       amqp_empty_table); // arguments
    state_->throwOnRPCError("AMQPQueue::setupChannel()", "Failed to setup basic consume.");
  }

  void
  AMQPQueue::ack(std::string const&filename)
  {
    auto dtag = fileToTag_.find(filename);
    if (dtag == fileToTag_.end()) {
      edm::Exception ex(edm::errors::OtherCMS);
      ex << "Asked to acknowledge an unknown filename: " << filename;
      ex.addContext("Calling AMQPQueue::ack()");
      throw ex;
    }
    if (state_->conn_ == nullptr) {
      edm::Exception ex(edm::errors::OtherCMS);
      ex << "AMQP used with invalid connection.";
      ex.addContext("Calling AMQPQueue::nack()");
      throw ex;
    }
    int res = amqp_basic_ack(state_->conn_, 1, dtag->second, 0);
    if (res != AMQP_STATUS_OK) {
      edm::Exception ex(edm::errors::OtherCMS);
      ex << "Failure when acknowledging file: " << filename;
      std::stringstream ss; ss << "AMQP error: " << amqp_error_string2(res);
      ex.addAdditionalInfo(ss.str());
      ex.addContext("Calling AMQPQueue::ack()");
      throw ex;
    }
    std::cout << "Gave a ACK for file " << filename << std::endl;
    fileToTag_.erase(dtag);
  }

  void
  AMQPQueue::nack(std::string const&filename)
  {
    auto dtag = fileToTag_.find(filename);
    if (dtag == fileToTag_.end()) {
      edm::Exception ex(edm::errors::OtherCMS);
      ex << "Asked to return an unknown filename: " << filename;
      ex.addContext("Calling AMQPQueue::nack()");
      throw ex;
    }
    if (state_->conn_ == nullptr) {
      edm::Exception ex(edm::errors::OtherCMS);
      ex << "AMQP used with invalid connection.";
      ex.addContext("Calling AMQPQueue::nack()");
      throw ex;
    }
    int res = amqp_basic_nack(state_->conn_, 1, dtag->second, 0, 1);
    if (res != AMQP_STATUS_OK) {
      edm::Exception ex(edm::errors::OtherCMS);
      ex << "Failure when returning file: " << filename;
      std::stringstream ss; ss << "AMQP error: " << amqp_error_string2(res);
      ex.addAdditionalInfo(ss.str());
      ex.addContext("Calling AMQPQueue::nack()");
      throw ex;
    }
    std::cout << "Gave a NACK for file " << filename << std::endl;
    fileToTag_.erase(dtag);
  }

}

