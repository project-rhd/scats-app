package scats.sparkApp.dataFramesSchema;

import java.io.Serializable;

/**
 * @author Yikai Gong
 */

public class SiteLayouts implements Serializable {
    private String NB_SCATS_SITE;
    private String DS_LOCATION;
    private String NB_DETECTOR;
    private String DT_GENERAL;
    private String NB_LANE;
    private String LANE_MVT;
    private String LOC_MVT;

    public SiteLayouts() {
    }

    public SiteLayouts(String NB_SCATS_SITE, String DS_LOCATION,
                       String NB_DETECTOR, String DT_GENERAL,
                       String NB_LANE, String LANE_MVT, String LOC_MVT) {
        this.NB_SCATS_SITE = NB_SCATS_SITE;
        this.DS_LOCATION = DS_LOCATION;
        this.NB_DETECTOR = NB_DETECTOR;
        this.DT_GENERAL = DT_GENERAL;
        this.NB_LANE = NB_LANE;
        this.LANE_MVT = LANE_MVT;
        this.LOC_MVT = LOC_MVT;
    }

    public String getNB_SCATS_SITE() {
        return NB_SCATS_SITE;
    }

    public void setNB_SCATS_SITE(String NB_SCATS_SITE) {
        this.NB_SCATS_SITE = NB_SCATS_SITE;
    }

    public String getDS_LOCATION() {
        return DS_LOCATION;
    }

    public void setDS_LOCATION(String DS_LOCATION) {
        this.DS_LOCATION = DS_LOCATION;
    }

    public String getDT_GENERAL() {
        return DT_GENERAL;
    }

    public void setDT_GENERAL(String DT_GENERAL) {
        this.DT_GENERAL = DT_GENERAL;
    }

    public String getNB_LANE() {
        return NB_LANE;
    }

    public void setNB_LANE(String NB_LANE) {
        this.NB_LANE = NB_LANE;
    }

    public String getLANE_MVT() {
        return LANE_MVT;
    }

    public void setLANE_MVT(String LANE_MVT) {
        this.LANE_MVT = LANE_MVT;
    }

    public String getLOC_MVT() {
        return LOC_MVT;
    }

    public void setLOC_MVT(String LOC_MVT) {
        this.LOC_MVT = LOC_MVT;
    }

    public String getNB_DETECTOR() {
        return NB_DETECTOR;
    }

    public void setNB_DETECTOR(String NB_DETECTOR) {
        this.NB_DETECTOR = NB_DETECTOR;
    }
}
