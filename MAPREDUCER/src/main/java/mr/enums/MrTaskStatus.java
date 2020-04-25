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
     * 计算等待
     */
    CALCULATE_WAIT(2),
    /**
     * 计算进行
     */
    IN_CALCULATION(3),
    /**
     * 计算完成
     */
    CALCULATE_OVER(4),
    ;

    private int code;

    MrTaskStatus(int code) {
        this.code = code;
    }

    public int getCode() {
        return code;
    }
}
