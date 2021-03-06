import FWCore.ParameterSet.Config as cms

rpcFEDIntegrity = cms.EDAnalyzer("RPCFEDIntegrity",
   RPCPrefixDir =  cms.untracked.string('RPC/FEDIntegrity'),
   RPCRawCountsInputTag = cms.untracked.InputTag('muonRPCDigis'),
   NumberOfFED = cms.untracked.int32(3),
   MinimumFEDID = cms.untracked.int32(790),
   MaximumFEDID= cms.untracked.int32(792)
 )


