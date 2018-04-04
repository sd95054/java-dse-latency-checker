import java.util.Arrays;

public class Statistics {
    long[] data;
    int size;

    public Statistics(long[] data) {
        this.data = data;
        size = data.length;
    }

    int getSize() {
        return size;
    }

    double getMean() {
        long sum = 0;
        for(long a : data)
            sum += a;
        return sum/size;
    }

    double getVariance() {
        double mean = getMean();
        double temp = 0;
        for(long a :data)
            temp += (a-mean)*(a-mean);
        return temp/(size-1);
    }

    double getStdDev() {
        return Math.sqrt(getVariance());
    }

    public double median() {
        Arrays.sort(data);

        if (data.length % 2 == 0) {
            return (data[(data.length / 2) - 1] + data[data.length / 2]) / 2.0;
        }
        return data[data.length / 2];
    }
}