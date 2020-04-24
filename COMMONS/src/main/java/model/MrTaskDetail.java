package model;

/**
 * MR任务详情
 */
public class MrTaskDetail {
    private String mrCode;
    private String mrInPath;
    private String mrStatus;

    public String getMrCode() {
        return mrCode;
    }

    public void setMrCode(String mrCode) {
        this.mrCode = mrCode;
    }

    public String getMrInPath() {
        return mrInPath;
    }

    public void setMrInPath(String mrInPath) {
        this.mrInPath = mrInPath;
    }

    public String getMrStatus() {
        return mrStatus;
    }

    public void setMrStatus(String mrStatus) {
        this.mrStatus = mrStatus;
    }

    @Override
    public String toString() {
        return "MrTaskDetail{" +
                "mrCode='" + mrCode + '\'' +
                ", mrInPath='" + mrInPath + '\'' +
                ", mrStatus='" + mrStatus + '\'' +
                '}';
    }
}
