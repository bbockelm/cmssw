#include "FWCore/Framework/interface/InputSourceMacros.h"
#include "FWCore/Sources/interface/VectorInputSourceMacros.h"
#include "PoolSource.h"
#include "QueueSource.h"
#include "EmbeddedRootSource.h"

using edm::PoolSource;
using edm::EmbeddedRootSource;
using edm::QueueSource;
DEFINE_FWK_INPUT_SOURCE(PoolSource);
DEFINE_FWK_INPUT_SOURCE(QueueSource);
DEFINE_FWK_VECTOR_INPUT_SOURCE(EmbeddedRootSource);
