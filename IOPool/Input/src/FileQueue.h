#ifndef IOPool_Input_FileQueue_h
#define IOPool_Input_FileQueue_h

/**
 * FileQueue: abstract interface representing a remote
 * queue that generates file names to process
 */

#include <string>

namespace edm {

  class FileQueue {
  public:
    FileQueue(FileQueue const&) = delete;
    FileQueue& operator=(FileQueue const&) = delete;

    virtual ~FileQueue();

    // Retrieve the next file name to process.
    // - `name`: Output of function, file name to be processed.  Should
    //   be fed to the InputFileCatalog to translate a possible LFN.
    // - Returns false if there are no more files to process.
    //     In this case, contents of `name` are undefined.
    // - Returns true if `name` was successfully set to a new
    //     filename.
    // - Throws an exception in case of error communicating with
    //     the remote queue.
    virtual bool next(std::string &name) = 0;

    // Acknowledges a file has been completely processed.
    // - `name`: File name, as provided by `next()`, that has been
    //   successfully processed.
    // - Should throw an exception if an error occurs when ACK'ing to
    //   the remote queue.
    virtual void ack(std::string const& name) = 0;

    // Note file processing failure; conventions the same as an ACK.
    virtual void nack(std::string const& name) = 0;

  protected:
    FileQueue() {}
  };

}

#endif
