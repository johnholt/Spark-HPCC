package org.hpccsystems.spark.temp;

import org.hpccsystems.ws.client.platform.DFUFileDetailInfo;
import org.hpccsystems.ws.client.gen.wsdfu.v1_36.DFUFilePartsOnCluster;


public class FilePartsFactory {

  public static DFUFilePartsOnClusterInfo[] makePartsOnCluster(
        DFUFileDetailInfo fd) {
    DFUFilePartsOnCluster[] cl = fd.getDFUFilePartsOnClusters();
    DFUFilePartsOnClusterInfo[] rslt = new DFUFilePartsOnClusterInfo[cl.length];
    for (int i=0; i<cl.length; i++) {
      rslt[i] = new DFUFilePartsOnClusterInfo(cl[i]);
    }
    return rslt;
  }

    private FilePartsFactory() {
    }

}
