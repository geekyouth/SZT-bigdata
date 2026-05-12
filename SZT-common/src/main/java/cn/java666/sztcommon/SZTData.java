package cn.java666.sztcommon;

import cn.hutool.core.io.FileUtil;
import cn.hutool.http.HttpUtil;
import org.junit.jupiter.api.Test;

public class SZTData {
    String SAVE_PATH = "/tmp/szt-data/szt-data-page-all.json";

    String appKey = "***";

    @Test
    public void saveData() {
        for (int i = 1; i <= 1337; i++) {
            String s = HttpUtil.get("https://opendata.sz.gov.cn/api/29200_00403601/1/service.xhtml?page=" + i + "&rows=1000&appKey=" + appKey);
            FileUtil.appendUtf8String(s + "\n", SAVE_PATH);
        }

        int size = FileUtil.readUtf8Lines(SAVE_PATH).size();

        if (size == 1337) {
            System.out.println(" 数据完全保存！！！ ");
        }
    }
}
