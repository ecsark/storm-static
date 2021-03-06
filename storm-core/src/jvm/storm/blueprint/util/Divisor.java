package storm.blueprint.util;

import java.io.Serializable;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;

/**
 * User: ecsark
 * Date: 5/31/14
 * Time: 10:26 PM
 */
public class Divisor implements Serializable {

    public static int getFirstDivisor (int data) {
        int len = (int)Math.sqrt(Math.abs(data));

        for (int i=2; i<=len; ++i) {
            if (data%i==0) {
                return i;
            }
        }

        return Math.abs(data);
    }

    public static List<Integer> getDivisors(int data) {
        List<Integer> primeList = new ArrayList<Integer>();
        int k = Math.abs(data);
        int sum = 1;

        if(isPrime(data)) {

            primeList.add(data);

        } else {

            int len = (int)Math.sqrt(Math.abs(data));
            for(int i = 2; i <= len; i++){

                if(isPrime(i)){

                    while(data % i == 0){
                        sum *= i;
                        if(sum <= k)
                            primeList.add(i);

                        data = data / i;
                        if(isPrime(data)){
                            primeList.add(data);
                            sum *= data;
                            break;
                        }
                    }
                    if(sum == k)
                        break;
                }
            }
        }
        return primeList;
    }


    public static boolean isPrime(int data) {
        for(int i = 2;i <= Math.sqrt(Math.abs(data)); i++){
            if (data % i == 0)
                return false;
        }
        return true;
    }


    public static int getGreatestCommonDivisor(List<Integer> data) {
        if (data.size() < 1) {
            throw new IllegalArgumentException("Input size should be greater than 0!");
        }

        BigInteger gcd = BigInteger.valueOf(data.get(0));

        for (int i=1; i<data.size(); ++i) {
            gcd = gcd.gcd(BigInteger.valueOf(data.get(i)));
        }

        return gcd.intValue();
    }

    public static long getLeastCommonMultiple(long a, long b) {
        long r = a, s = a, t = b;
        if (a < b) {
            r = a;
            a = b;
            b = r;
        }
        while (r != 0) {
            r = a % b;
            a = b;
            b = r;
        }
        return s * t / a;
    }


    public static long getLeastCommonMultiple (List<Integer> data) {
        if (data.size() < 1) {
            throw new IllegalArgumentException("Input size should be greater than 0!");
        }

        long lcm = data.get(0);
        for (int i=1; i<data.size(); ++i) {
            lcm = getLeastCommonMultiple(lcm, data.get(i));
        }

        return lcm;
    }
}
