package cn.java666.sztcommon;

import cn.hutool.core.io.FileUtil;
import cn.hutool.http.HttpUtil;
import org.junit.Test;

/**
 * @author Geek
 * @date 2020-04-13 19:11:04
 *
 * 由于在前期保存数据时，我没有采用合适的格式分隔，
 * 导致后来 ETL 非常繁琐，此处建议使用最接近原始数据的格式保存，
 * 原始数据：  参考 .file/.api/page1x100.json（压缩过的 json 保存，不要格式化后保存）
 *
 * 因为 spark 可以直接处理多行 json 文本，
 * 而这里的 json 默认每行存一个完整 json 对象文本，但是单个 json 对象却包含 100 个子节点，是无法通过 spark 直接读取的。
 * 后续还得拆解。
 */
public class SZTData {
    String SAVE_PATH = "/tmp/szt-data/szt-data-page-all.json";

    // TODO appKey 自己申请 https://opendata.sz.gov.cn/data/api/toApiDetails/29200_00403601
    String appKey = "***";

    /**
     * 这个过程可能花费一个通宵，如果中断，查看已保存数据最后一条的 page，然后调整 i 的起始值继续抓取
     * 使用 @test 可以保存每次运行的历史日志
     */
    @Test
    public void saveData() {
        for (int i = 1; i <= 1337; i++) {
            String s = HttpUtil.get("https://opendata.sz.gov.cn/api/29200_00403601/1/service.xhtml?page=" + i + "&rows=1000&appKey=" + appKey);
            // 一定要加换行符，否则以后处理起来会是灾难。
            // 一定要加换行符，否则以后处理起来会是灾难。
            // 一定要加换行符，否则以后处理起来会是灾难。
            FileUtil.appendUtf8String(s + "\n", SAVE_PATH);
        }

        int size = FileUtil.readUtf8Lines(SAVE_PATH).size();

        // 如果中途断了，需要自己实现数据完整性检查
        if (size == 1337) {
            System.out.println(" 数据完全保存！！！ ");
        }
    }
}
