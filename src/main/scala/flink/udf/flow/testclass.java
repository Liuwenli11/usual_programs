package flink.udf.flow;

public class testclass {
    private int factor = 2;
    public testclass() {
        this.factor = factor;
    }
    public int abc(int s) {
        return s + factor;
    }
}
