package backend.se;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import static java.lang.Math.log;

/**
 * This class represents the variable bytes coding algorithm.
 */
class VarBytesCoder {
    /**
     * Encodes a int number to byte[] based on variable byte coding algorithm.
     * @param n The integer to be encoded.
     * @return A byte array which is the encoded n.
     */
    private static byte[] encodeNumber(int n) {
        if (n == 0) {
            return new byte[]{0};
        }
        int i = (int) (log(n) / log(128)) + 1;
        byte[] rv = new byte[i];
        int j = i - 1;
        do {
            rv[j--] = (byte) (n % 128);
            n /= 128;
        } while (j >= 0);
        rv[i - 1] += 128;
        return rv;
    }

    /**
     * Encodes a list of integer numbers into bytes aray
     * @param numbers a list of integers to be encoded
     * @return a bytes array encoded from numbers
     */
    static byte[] encode(List<Integer> numbers) {
        ByteBuffer buf = ByteBuffer.allocate(numbers.size() * (Integer.SIZE / Byte.SIZE) + 64);
        for (Integer number : numbers) {
            buf.put(encodeNumber(number));
        }
        buf.flip();
        byte[] rv = new byte[buf.limit()];
        buf.get(rv);
        return rv;
    }

    public static byte[] encodeInterpolate(List<Integer> numbers) {
        ByteBuffer buf = ByteBuffer.allocate(numbers.size() * (Integer.SIZE / Byte.SIZE) + 1024);
        int last = -1;
        for (int i = 0; i < numbers.size(); i++) {
            Integer num = numbers.get(i);
            if (i == 0) {
                buf.put(encodeNumber(num));
            } else {
                buf.put(encodeNumber(num - last));
            }
            last = num;
        }

        buf.flip();
        byte[] rv = new byte[buf.limit()];
        buf.get(rv);
        return rv;
    }

    /**
     * Decodes a bytes array into a list of integers.
     * @param byteStream bytes array to be decoded.
     * @return a list of  integer numbers decoded from byteStream.
     */
    public static List<Integer> decode(byte[] byteStream) {
        List<Integer> numbers = new ArrayList<>();
        int n = 0;
        for (byte b : byteStream) {
            if ((b & 0xff) < 128) {
                n = 128 * n + b;
            } else {
                int num = (128 * n + ((b - 128) & 0xff));
                numbers.add(num);
                n = 0;
            }
        }
        return numbers;
    }

    public static List<Integer> vbDecode(byte[] byteStream){
        List<Integer> numbers = new ArrayList<>();
        int n = 0;
        for(byte b : byteStream){
            //Byte b = code.poll(); // read leading byte
            //System.out.println(" Reading byte " + b.toString() );

            int  bi = b & 0xff;  // decimal value of this byte
            if (bi < 128) {       //continuation bit is set to 0
                n = 128 * n + bi;
            } else {              // continuation bit is set to 1
                n = 128 * n + (bi - 128);
                numbers.add(n);   // number is stored
                n = 0;            // reset
            }
        }
        return numbers;
    }

    public static List<Integer> decodeFirstHalf(byte[] byteStream) {
        List<Integer> numbers = new ArrayList<>();
        int n = 0;
        int counter = 0;
        int bytesCounter = 0;
        for (byte b : byteStream) {
            bytesCounter += 1;
            if ((b & 0xff) < 128) {
                n = 128 * n + b;
            } else {
                int num = (128 * n + ((b - 128) & 0xff));
                numbers.add(num);
                counter += 1;
                n = 0;
            }
            if(counter == IndexBuilder.BLOCK_SIZE) break;
        }
        numbers.add(bytesCounter);
        return numbers;
    }


}
