package cn.java666.sztcommon.pojo;

import lombok.AllArgsConstructor;
import lombok.Data;

/**
 * @author Geek
 * @date 2020-04-29 23:15:45
 */
// @Builder
@AllArgsConstructor
@Data
public class SztDataBean {
	private String deal_date;
	private String close_date;
	private String card_no;
	private String deal_value;
	private String deal_type;
	private String company_name;
	private String car_no;
	private String station;
	private String conn_mark;
	private String deal_money;
	private String equ_no;
}
