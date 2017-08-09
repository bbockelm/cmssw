#ifndef Geometry_TrackerNumberingBuilder_CmsTrackerWheelBuilder_H
#define Geometry_TrackerNumberingBuilder_CmsTrackerWheelBuilder_H

#include "Geometry/TrackerNumberingBuilder/plugins/CmsTrackerLevelBuilder.h"
#include "FWCore/ParameterSet/interface/types.h"
#include <string>

/**
 * Class which builds TEC wheels
 */
class CmsTrackerWheelBuilder : public CmsTrackerLevelBuilder {
  
 private:
  void sortNS(DDFilteredView& , GeometricDet*) override;
  void buildComponent(DDFilteredView& , GeometricDet*, std::string) override;

};

#endif
