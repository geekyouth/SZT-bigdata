package cn.java666.sztcommon

/**
 * @author Geek
 * @date 2020-04-14 04:15:35
 *
 * 源数据样例类，最小数据单元
 */
case class SZTBean(
	deal_date: String, 			//交易时间，格式化的日期时间 			2018-09-01 11:19:04
	close_date: String,			//结算时间，次日起始时间0点				2018-09-01 00:00:00
	card_no: String, 			//卡号，9位字母						FFEBEBAJH
	deal_value: String, 		//交易金额，整数分值，原价				200
	deal_type: String, 			//交易类型，汉字描述 					地铁入站|地铁出站|巴士
	company_name: String, 		//公司名称，线名						巴士集团|地铁七号线		
	car_no: String, 			//车号								01563D|宽AGM26-27
	station: String, 			//站名								74路|华强北
	conn_mark: String, 			//联程标记							0 直达 |1 联程
	deal_money: String, 		//实收金额，整数分值，优惠后			190
	equ_no: String 				//闸机号								265030122
)

/*

"card_no": "FFEBEBAJH",
"deal_date": "2018-09-01 11:19:04",
"deal_type": "地铁入站",
"deal_money": "0",
"deal_value": "0",
"equ_no": "265028117"
"company_name": "地铁七号线",
"station": "赤尾",
"car_no": "进AGM19-20",
"conn_mark": "0",
"close_date": "2018-09-01 00:00:00",

"deal_date": "2018-09-01 11:15:07",
"close_date": "2018-09-01 00:00:00",
"card_no": "FHHEBGGFI",
"deal_value": "200",
"deal_type": "地铁出站",
"company_name": "地铁七号线",
"car_no": "宽AGM26-27",
"station": "华强北",
"conn_mark": "0",
"deal_money": "190",
"equ_no": "265030122"

"deal_date": "2018-09-01 10:06:46",
"close_date": "2018-09-01 00:00:00",
"card_no": "FIAIAIGAD",
"deal_value": "250",
"deal_type": "巴士",
"company_name": "巴士集团",
"car_no": "01563D",
"station": "74路",
"conn_mark": "0",
"deal_money": "200",
"equ_no": "231010161"

*/