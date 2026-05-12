package cn.java666.szthbase.service;

import cn.java666.sztcommon.pojo.SztDataBean;
import cn.java666.szthbase.dao.SztDataDao;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.stereotype.Service;

import java.io.IOException;

import static cn.java666.sztcommon.enums.SztEnum.*;

@Slf4j
@Service
public class SztDataService {
    private final String tb = SZT_TABLE_NAME.value();
    private final String cf = SZT_TABLE_CF.value();

    private final SztDataDao dao;

    public SztDataService(SztDataDao dao) {
        this.dao = dao;
    }

    public void insert(SztDataBean data) {
        try {
            String cardNoRe = StringUtils.reverse(data.getCardNo());
            dao.putCell(tb, cardNoRe, cf, SZT_CL_DEAL_DATE.value(), data.getDealDate());
            dao.putCell(tb, cardNoRe, cf, SZT_CL_CLOSE_DATE.value(), data.getCloseDate());
            dao.putCell(tb, cardNoRe, cf, SZT_CL_CARD_NO.value(), data.getCardNo());
            dao.putCell(tb, cardNoRe, cf, SZT_CL_DEAL_VALUE.value(), data.getDealValue());
            dao.putCell(tb, cardNoRe, cf, SZT_CL_DEAL_TYPE.value(), data.getDealType());
            dao.putCell(tb, cardNoRe, cf, SZT_CL_LINE.value(), data.getCompanyName());
            dao.putCell(tb, cardNoRe, cf, SZT_CL_CAR_NO.value(), data.getCarNo());
            dao.putCell(tb, cardNoRe, cf, SZT_CL_STATION.value(), data.getStation());
            dao.putCell(tb, cardNoRe, cf, SZT_CL_CONN_MARK.value(), data.getConnMark());
            dao.putCell(tb, cardNoRe, cf, SZT_CL_DEAL_MONEY.value(), data.getDealMoney());
            dao.putCell(tb, cardNoRe, cf, SZT_CL_EQU_NO.value(), data.getEquNo());
            log.warn("写入 hbase 成功 [{}]", data);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
