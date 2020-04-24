package mr.enums;

/**
 * 任务状态
 */
public enum MrTaskStatus {
    /**
     * 转发完成
     */
    TRANSFER_OVER(0),
    /**
     * 任务激活
     */
    ACTIVE(1),
    /**
     * 计算进行
     */
    IN_CALCULATION(2),
    /**
     * 计算完成
     */
    CALCULATE_OVER(3),
    ;

    private int code;

    MrTaskStatus(int code) {
        this.code = code;
    }

    public int getCode() {
        return code;
    }
}
