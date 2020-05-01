package cn.java666.szthbase.service;

import cn.java666.sztcommon.pojo.SztDataBean;
import cn.java666.szthbase.dao.SztDataDao;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.io.IOException;

import static cn.java666.sztcommon.enums.SztEnum.*;

/**
 * @author Geek
 * @date 2020-04-29 22:19:33
 * 业务逻辑层
 */
@Slf4j
@Service
public class SztDataService {
	private String tb = SZT_TABLE_NAME.value();
	private String cf = SZT_TABLE_CF.value();
	
	@Resource
	private SztDataDao dao;
	
	/** 插入新数据 */
	public void insert(SztDataBean data) {
		try {
			String card_no_re = StringUtils.reverse(data.getCard_no());
			dao.putCell(tb, card_no_re, cf, SZT_CL_DEAL_DATE.value(), data.getDeal_date());
			dao.putCell(tb, card_no_re, cf, SZT_CL_CLOSE_DATE.value(), data.getClose_date());
			dao.putCell(tb, card_no_re, cf, SZT_CL_CARD_NO.value(), data.getCard_no());
			dao.putCell(tb, card_no_re, cf, SZT_CL_DEAL_VALUE.value(), data.getDeal_value());
			dao.putCell(tb, card_no_re, cf, SZT_CL_DEAL_TYPE.value(), data.getDeal_type());
			dao.putCell(tb, card_no_re, cf, SZT_CL_LINE.value(), data.getCompany_name());
			dao.putCell(tb, card_no_re, cf, SZT_CL_CAR_NO.value(), data.getCar_no());
			dao.putCell(tb, card_no_re, cf, SZT_CL_STATION.value(), data.getStation());
			dao.putCell(tb, card_no_re, cf, SZT_CL_CONN_MARK.value(), data.getConn_mark());
			dao.putCell(tb, card_no_re, cf, SZT_CL_DEAL_MONEY.value(), data.getDeal_money());
			dao.putCell(tb, card_no_re, cf, SZT_CL_EQU_NO.value(), data.getEqu_no());
			log.warn("写入 hbase 成功 [{}]", data);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}
