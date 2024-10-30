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

    // Getters
    public String getRn() {
        return rn;
    }

    public String getNextStpId() {
        return nextStpId;
    }

    public String getArrT() {
        return arrT;
    }

    public String getDestNm() {
        return destNm;
    }

    public String getDestSt() {
        return destSt;
    }
    
    public String getNextStaNm() {
        return nextStaNm;
    }

    public String getNextStaId() {
        return nextStaId;
    }


    // Optionally, you can add more getters for other fields if needed
}
