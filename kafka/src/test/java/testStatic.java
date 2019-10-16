import com.xxd.test.A;

import java.util.List;

public class testStatic {
    public static void main(String[] args) {
        List<String> list = A.list;
        List<String> list1 = A.list;
        System.out.println(list == list1);
    }
}
