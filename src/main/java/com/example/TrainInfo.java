package com.example;

public class TrainInfo {
    private String rn;
    private String destSt;
    private String destNm;
    private String trDr;
    private String nextStaId;
    private String nextStpId;
    private String nextStaNm;
    private String prdt;
    private String arrT;
    private String isApp;
    private String isDly;
    private String lat;
    private String lon;
    private String heading;

    // Constructor
    public TrainInfo(String rn, String destSt, String destNm, String trDr, String nextStaId,
                     String nextStpId, String nextStaNm, String prdt, String arrT,
                     String isApp, String isDly, String lat, String lon, String heading) {
        this.rn = rn;
        this.destSt = destSt;
        this.destNm = destNm;
        this.trDr = trDr;
        this.nextStaId = nextStaId;
        this.nextStpId = nextStpId;
        this.nextStaNm = nextStaNm;
        this.prdt = prdt;
        this.arrT = arrT;
        this.isApp = isApp;
        this.isDly = isDly;
        this.lat = lat;
        this.lon = lon;
        this.heading = heading;
    }

    @Override
    public String toString() {
        return "TrainInfo{" +
                "rn='" + rn + '\'' +
                ", destSt='" + destSt + '\'' +
                ", destNm='" + destNm + '\'' +
                ", trDr='" + trDr + '\'' +
                ", nextStaId='" + nextStaId + '\'' +
                ", nextStpId='" + nextStpId + '\'' +
                ", nextStaNm='" + nextStaNm + '\'' +
                ", prdt='" + prdt + '\'' +
                ", arrT='" + arrT + '\'' +
                ", isApp='" + isApp + '\'' +
                ", isDly='" + isDly + '\'' +
                ", lat='" + lat + '\'' +
                ", lon='" + lon + '\'' +
                ", heading='" + heading + '\'' +
                '}';
    }
}