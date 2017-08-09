// -*- C++ -*-
//
// Package:    EcalDigiToRaw
// Class:      EcalDigiToRaw
// 
/**\class EcalDigiToRaw EcalDigiToRaw.cc EventFilter/EcalDigiToRaw/src/EcalDigiToRaw.cc

 Description: <one line class summary>

 Implementation:
     <Notes on implementation>
*/
//
// Original Author:  Emmanuelle Perez
//         Created:  Sat Nov 25 13:59:51 CET 2006
//
//


// system include files


// user include files
#include "EventFilter/EcalDigiToRaw/interface/EcalDigiToRaw.h"

#include "DataFormats/EcalDetId/interface/EcalDetIdCollections.h"

#include "DataFormats/FEDRawData/interface/FEDRawDataCollection.h"
#include "DataFormats/FEDRawData/interface/FEDRawData.h"
#include "DataFormats/FEDRawData/interface/FEDNumbering.h"

// #include "DataFormats/Common/interface/Handle.h"
#include "DataFormats/Common/interface/Handle.h"
#include "FWCore/Framework/interface/EventSetup.h"
#include "FWCore/Framework/interface/ESHandle.h"

#include "Geometry/EcalMapping/interface/EcalElectronicsMapping.h"
#include "Geometry/EcalMapping/interface/EcalMappingRcd.h"


//use make_unique

using namespace edm;
using namespace std;

EcalDigiToRaw::EcalDigiToRaw(const edm::ParameterSet& iConfig) :
   doTCC_(iConfig.getUntrackedParameter<bool>("WriteTCCBlock")),
   doSR_(iConfig.getUntrackedParameter<bool>("WriteSRFlags")),
   doTower_(iConfig.getUntrackedParameter<bool>("WriteTowerBlock")),
   doBarrel_(iConfig.getUntrackedParameter<bool>("DoBarrel")),
   doEndCap_(iConfig.getUntrackedParameter<bool>("DoEndCap")),
   listDCCId_(iConfig.getUntrackedParameter< std::vector<int32_t> >("listDCCId")),
   label_(iConfig.getParameter<string>("Label")),
   instanceNameEB_(iConfig.getParameter<string>("InstanceEB")),
   instanceNameEE_(iConfig.getParameter<string>("InstanceEE")),
   EBDigiToken_(consumes<EBDigiCollection>(edm::InputTag(label_,instanceNameEB_))),
   EEDigiToken_(consumes<EEDigiCollection>(edm::InputTag(label_,instanceNameEE_))),
   labelTT_(consumes<EcalTrigPrimDigiCollection>(iConfig.getParameter<edm::InputTag>("labelTT"))),
   labelEBSR_(consumes<EBSrFlagCollection>(iConfig.getParameter<edm::InputTag>("labelEBSRFlags"))),
   labelEESR_(consumes<EESrFlagCollection>(iConfig.getParameter<edm::InputTag>("labelEESRFlags"))), 
   debug_(iConfig.getUntrackedParameter<bool>("debug")),
   Towerblockformatter_(std::make_unique<TowerBlockFormatter>(this)), 
   TCCblockformatter_(std::make_unique<TCCBlockFormatter>(this)),
   Headerblockformatter_(std::make_unique<BlockFormatter>(this)),
   SRblockformatter_(std::make_unique<SRBlockFormatter>(this))
{
   produces<FEDRawDataCollection>();
}



//
// member functions
//

// ------------ method called to for each event  ------------
void
EcalDigiToRaw::produce(edm::StreamID id, edm::Event& iEvent, const edm::EventSetup& iSetup) const
{
// change to loginfo 
   if (debug_) LogInfo("EcalDigiToRaw: ") << "Enter in EcalDigiToRaw::produce ... " << endl;

   ESHandle< EcalElectronicsMapping > ecalmapping;
   iSetup.get< EcalMappingRcd >().get(ecalmapping);
   const EcalElectronicsMapping* TheMapping = ecalmapping.product();

   auto local = Towerblockformatter_ -> StartEvent();
   auto header = SRblockformatter_ -> StartEvent();

   auto runnum = iEvent.id().run();


  auto counter = iEvent.id().event();
  auto bx = iEvent.bunchCrossing();
  auto orbitnumber = iEvent.orbitNumber();

  auto lv1 = counter % (0x1<<24);

  auto productRawData = std::make_unique<FEDRawDataCollection>();


  Headerblockformatter_ -> DigiToRaw(productRawData.get(), runnum, orbitnumber, bx, lv1);


// ---------   Now the Trigger Block part

  Handle<EcalTrigPrimDigiCollection> ecalTrigPrim;
  
  Handle<EBSrFlagCollection> ebSrFlags;
  Handle<EESrFlagCollection> eeSrFlags;


  if (doTCC_) {

     if (debug_) LogInfo("EcalDigiToRaw: ") << "Creation of the TCC block  " << endl;
     // iEvent.getByType(ecalTrigPrim);
	iEvent.getByToken(labelTT_, ecalTrigPrim);

     // loop on TP's and add one by one to the block
     for (EcalTrigPrimDigiCollection::const_iterator it = ecalTrigPrim -> begin();
			   it != ecalTrigPrim -> end(); it++) {

	   const EcalTriggerPrimitiveDigi& trigprim = *it;
	   const EcalTrigTowerDetId& detid = it -> id();

	   if ( (detid.subDet() == EcalBarrel) && (! doBarrel_) ) continue;
           if ( (detid.subDet() == EcalEndcap) && (! doEndCap_) ) continue;

	   int iDCC = TheMapping -> DCCid(detid);
           int FEDid = FEDNumbering::MINECALFEDID + iDCC;

           FEDRawData& rawdata = productRawData.get() -> FEDData(FEDid);
	   
	   // adding the primitive to the block
	   TCCblockformatter_ -> DigiToRaw(trigprim, rawdata, TheMapping, bx, lv1);

     }   // end loop on ecalTrigPrim

   }  // endif doTCC


   if (doSR_) {	
	if (debug_) LogInfo("EcalDigiToRaw: ") << " Process the SR flags " << endl;

	if (doBarrel_) {

        // iEvent.getByType(ebSrFlags);
	   iEvent.getByToken(labelEBSR_, ebSrFlags);
                                                                                                                                                
           for (EBSrFlagCollection::const_iterator it = ebSrFlags -> begin();
                        it != ebSrFlags -> end(); it++) {
                const EcalSrFlag& srflag = *it;
		int flag = srflag.value();

		EcalTrigTowerDetId id = srflag.id();
		int Dccid = TheMapping -> DCCid(id);
		int DCC_Channel = TheMapping -> iTT(id);
		int FEDid = FEDNumbering::MINECALFEDID + Dccid;
		// if (Dccid == 10) LogInfo("EcalDigiToRaw: ") << "Dcc " << Dccid << " DCC_Channel " << DCC_Channel << " flag " << flag << endl;
		if (debug_) LogInfo("EcalDigiToRaw: ") << "will process SRblockformatter_ for FEDid " << dec << FEDid << endl;
		FEDRawData& rawdata = productRawData.get() -> FEDData(FEDid);
		if (debug_) Headerblockformatter_ -> print(rawdata);
		SRblockformatter_ -> DigiToRaw(Dccid,DCC_Channel,flag, rawdata, bx, lv1, header);

           }
	}  // end DoBarrel


	if (doEndCap_) {
	// iEvent.getByType(eeSrFlags);
	iEvent.getByToken(labelEESR_, eeSrFlags);

           for (EESrFlagCollection::const_iterator it = eeSrFlags -> begin();
                        it != eeSrFlags -> end(); it++) {
                const EcalSrFlag& srflag = *it;
                int flag = srflag.value();
		EcalScDetId id = srflag.id();
		pair<int, int> ind = TheMapping -> getDCCandSC(id);
		int Dccid = ind.first;
		int DCC_Channel = ind.second;

                int FEDid = FEDNumbering::MINECALFEDID + Dccid;
                FEDRawData& rawdata = productRawData.get() -> FEDData(FEDid);
                SRblockformatter_ -> DigiToRaw(Dccid,DCC_Channel,flag, rawdata, bx, lv1, header);
           }
	}  // end doEndCap

   }   // endif doSR


// ---------  Now the Tower Block part

  Handle<EBDigiCollection> ebDigis;
  Handle<EEDigiCollection> eeDigis;

  if (doTower_) {

	if (doBarrel_) {
   	if (debug_) LogInfo("EcalDigiToRaw: ") << "Creation of the TowerBlock ... Barrel case " << endl;
        iEvent.getByToken(EBDigiToken_,ebDigis);
        for (EBDigiCollection::const_iterator it=ebDigis -> begin();
                                it != ebDigis->end(); it++) {
                const EBDataFrame& dataframe = *it;
                const EBDetId& ebdetid = it -> id();
		int DCCid = TheMapping -> DCCid(ebdetid);
        	int FEDid = FEDNumbering::MINECALFEDID + DCCid ;
                FEDRawData& rawdata = productRawData.get() -> FEDData(FEDid);
                Towerblockformatter_ -> DigiToRaw(dataframe, rawdata, TheMapping, bx, lv1-1, local);
        }

	}

	if (doEndCap_) {
	if (debug_) LogInfo("EcalDigiToRaw: ") << "Creation of the TowerBlock ... EndCap case " << endl;
        iEvent.getByToken(EEDigiToken_,eeDigis);
        for (EEDigiCollection::const_iterator it=eeDigis -> begin();
                                it != eeDigis->end(); it++) {
                const EEDataFrame& dataframe = *it;
                const EEDetId& eedetid = it -> id();
		EcalElectronicsId elid = TheMapping -> getElectronicsId(eedetid);
                int DCCid = elid.dccId() ;   
                int FEDid = FEDNumbering::MINECALFEDID + DCCid;
                FEDRawData& rawdata = productRawData.get() -> FEDData(FEDid);
                Towerblockformatter_ -> DigiToRaw(dataframe, rawdata, TheMapping, bx, lv1-1, local);
        }
	}

  }  // endif doTower_



// -------- Clean up things ...

  //map<int, map<int,int> > FEDorder = local.FEDorder;

  Headerblockformatter_ -> CleanUp(*(productRawData.get()), local.fedorder);


/*
   LogInfo("EcalDigiToRaw: ") << "For FED 633 " << endl;
         FEDRawData& rawdata = productRawData -> FEDData(633);
         Headerblockformatter_ -> print(rawdata);
*/

 // Headerblockformatter_ -> PrintSizes(productRawData.get());



 Towerblockformatter_ -> EndEvent(productRawData.get());

 iEvent.put(std::move(productRawData));


 return;

}

//
//






