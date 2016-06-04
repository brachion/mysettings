
import static jp.co.alpha._117.tool.cichlid.common.util.DateUtil.*;

import java.util.Calendar;
import java.util.Date;
import java.util.Iterator;
import java.util.List;

import jp.co.alpha._117.tool.cichlid.common.entity.Column;

import org.apache.commons.collections.iterators.ArrayListIterator;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.time.DateUtils;
import org.apache.commons.lang.time.FastDateFormat;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.SqlParameterSource;
import org.springframework.util.Assert;

/**
 * 検索用SQLを作成するビルダクラスです。
 * 
 * @author  kurisakisatoshi
 * @version $Revision: 443 $
 */
public final class SelectSqlBuilder {
    
    /**
     * ビルダクラスを生成します。
     * 
     * @return インスタンス
     */
    public static SelectSqlBuilder builder() {
        return new SelectSqlBuilder();
    }
    
    /**
     * 指定の文字列をSQL文に追加します。
     * 
     * @param str 追加する文字列
     * @return インスタンス
     */
    public SelectSqlBuilder add(String str) {
        
        Assert.notNull(str, "str must not be null.");
        
        buffer.append(StringUtils.stripEnd(str, null)).append(" ");
        
        return this;
    }
    
    /**
     * 指定の数値をSQL文に追加します。
     * 
     * @param i 追加する数値
     * @return インスタンス
     */
    public SelectSqlBuilder add(int i) {
        return add(String.valueOf(i));
    }
    
    /**
     * 指定の数値をSQL文に追加します。
     * 
     * @param l 追加する数値
     * @return インスタンス
     */
    public SelectSqlBuilder add(long l) {
        return add(String.valueOf(l));
    }
    
    /**
     * 指定の文字列をカンマ区切りでSQL文に追加します。
     * 
     * @param str 追加する文字列
     * @return インスタンス
     */
    public SelectSqlBuilder add(String... str) {
        
        Assert.notEmpty(str, "str must not be null or empty.");
        
        for (int i=0; i<str.length; i++) {
            add(str[i]);
            
            if (i != str.length -1) {
                add(",");
            }
        }
        
        return this;
    }
    
    /**
     * 指定のカラムをSQL文に追加します。
     * 
     * @param column 追加するカラム
     * @return インスタンス
     */
    public SelectSqlBuilder add(Column column) {
        
        Assert.notNull(column, "column must not be null.");
        
        add(column.getColumnName());
        
        return this;
    }
    
    /**
     * 指定のカラムをカンマ区切りでSQL文に追加します。
     * 
     * @param columns 追加するカラム
     * @return インスタンス
     */
    public SelectSqlBuilder add(Column... columns) {
        
        Assert.notEmpty(columns, "columns must not be null or empty.");
        
        for (int i=0; i<columns.length; i++) {
            add(columns[i]);
            
            if (i != columns.length -1) {
                add(",");
            }
        }
        
        return this;
    }
    
    /**
     * 検索結果の最大行数の指定をSQL文に追加します。
     * マイナスの値の場合何もしない
     * 
     * @param limitCount 最大行数
     * @return インスタンス
     */
    public SelectSqlBuilder limit(int limitCount) {
        
        if (limitCount < 0) {
            return this;
        }
        
        add("limit :" + index.getValue());
        paramSource.addValue(index.getValue() + "", limitCount);
        index.increment();
        
        return this;
    }
    
    
    /**
     * バインド値をSQLに追加します。
     * 
     * @param value バインド値
     * @return インスタンス
     */
    public SelectSqlBuilder addBindValue(Object value) {
        
        add(":" + index.getValue());
        paramSource.addValue(index.getValue() + "", value);
        index.increment();
        
        return this;
    }
    
    
    
    /**
     * 指定の条件を作成しSQL文に追加します。
     * <p><code>null</code>(または空文字)を検索条件に指定したい場合は{@link #add(String)}を使用してください。
     * <pre>
     * 例)
     * add("col is null");
     * </pre>
     * 
     * @param columnName カラム
     * @param condition 条件
     * @param type 検索条件種別
     * @return インスタンス
     */
    public SelectSqlBuilder whereInt(String columnName, int condition, ConditionType type) {
        
        where(columnName, condition, type);
        
        return this;
    }
    
    /**
     * 指定の条件を作成しSQL文に追加します。
     * <p><code>null</code>(または空文字)を検索条件に指定したい場合は{@link #add(String)}を使用してください。
     * <pre>
     * 例)
     * add("col is null");
     * </pre>
     * 
     * @param columnName カラム
     * @param condition 条件
     * @param type 検索条件種別
     * @return インスタンス
     */
    public SelectSqlBuilder whereLong(String columnName, long condition, ConditionType type) {
        
        where(columnName, condition, type);
        
        return this;
    }
    
    /**
     * 指定の条件を作成しSQL文に追加します。
     * <p><code>null</code>(または空文字)を検索条件に指定したい場合は{@link #add(String)}を使用してください。
     * <pre>
     * 例)
     * add("col is null");
     * </pre>
     * 
     * <p>注意<br>
     * oracleでは空文字は<code>null</code>で扱われるため、<code>condition</code>に空文字を指定してもヒットしません。
     * <code>condition</code>に空文字が指定された場合はパラメータエラーとなります。<br>
     * 
     * @param columnName カラム
     * @param condition 条件
     * @param type 検索条件種別
     * @return インスタンス
     */
    public SelectSqlBuilder whereString(String columnName, String condition, ConditionType type) {
        
        where(columnName, type.escape(String.valueOf(condition)), type);
        
        return this;
    }
    
    /**
     * 指定の条件を作成しSQL文に追加します。
     * 
     * @param columnName カラム
     * @param condition 条件
     * @param type 検索種別
     */
    private void where(String columnName, Object condition, ConditionType type) {
        
        Assert.isTrue(StringUtils.isNotBlank(columnName), "columnName must not be null or blank.");
        Assert.notNull(condition, "condition must not be null.");
        Assert.notNull(type, "type must not be null.");
        
        add(type.makeCondition(columnName, ":" + index.getValue()));
        paramSource.addValue(index.getValue() + "", condition);
        index.increment();
    }
    
    
    
    /**
     * 指定の条件を作成しSQL文に追加します。
     * <p><code>conditions</code>の値は1000件に分割され
     * 以下のような条件を付与します。
     * 
     * <pre>
     * (col in (n1,n2,・・,n1000) or col in (n1001,n1002,・・,n2000) or ・・・)
     * </pre>
     * 
     * @param columnName カラム
     * @param conditions 条件
     * @return インスタンス
     */
    public SelectSqlBuilder whereIntList(String columnName, List<Integer> conditions) {
        
        whereIn(columnName, conditions);
        
        return this;
    }
    
    /**
     * 指定の条件を作成しSQL文に追加します。
     * <p><code>conditions</code>の値は1000件に分割され
     * 以下のような条件を付与します。
     * 
     * <pre>
     * (col in (n1,n2,・・,n1000) or col in (n1001,n1002,・・,n2000) or ・・・)
     * </pre>
     * 
     * @param columnName カラム
     * @param conditions 条件
     * @return インスタンス
     */
    public SelectSqlBuilder whereLongList(String columnName, List<Long> conditions) {
        
        whereIn(columnName, conditions);
        
        return this;
    }
    
    /**
     * 指定の条件を作成しSQL文に追加します。
     * <p><code>conditions</code>の値は1000件に分割され
     * 以下のような条件を付与します。
     * 
     * <pre>
     * (col in (n1,n2,・・,n1000) or col in (n1001,n1002,・・,n2000) or ・・・)
     * </pre>
     * 
     * @param columnName カラム
     * @param conditions 条件
     * @return インスタンス
     */
    public SelectSqlBuilder whereStringList(String columnName, List<String> conditions) {
        
        whereIn(columnName, conditions);
        
        return this;
    }
    
    /**
     * 指定の条件を作成しSQL文に追加します。
     * 
     * @param columnName カラム
     * @param conditions 条件
     */
    private void whereIn(String columnName, List<? extends Object> conditions) {
        
        Assert.isTrue(!StringUtils.isEmpty(columnName), "columnName must not be null or empty.");
        Assert.notEmpty(conditions, "conditions must not be null or empty.");
        
        add("(");
        
        int size = conditions.size();
        for (int i=0; i*1000<size; i++) {      // 1000で分割
            
            if (i != 0) {
                add("OR");
            }
            
            add(columnName);
            add("IN(");
            
            int start = i*1000;
            int end = (((i+1)*1000) <= size) ? ((i+1)*1000) : (i*1000 + size%1000);
            @SuppressWarnings("unchecked")     // ArrayListIteratorがジェネリックス未対応のためunchecked
            Iterator<Object> iter = new ArrayListIterator(conditions.toArray(), start, end);
            while (iter.hasNext()) {
                
                add(":" + index.getValue());
                paramSource.addValue(index.getValue() + "", iter.next());
                index.increment();
                
                if (iter.hasNext()) {
                    add(",");
                }
            }
            
            add(")");
        }
        
        add(")");
    }
    
    
    
    /**
     * 指定の条件を作成しSQL文に追加します。
     * 
     * @param columnName カラム
     * @param min 下限
     * @param max 上限
     * @return オブジェクト
     */
    public SelectSqlBuilder whereBetween(String columnName, int min, int max) {
        
        Assert.isTrue(min <= max, "min must be less or equals max. min=" + min + ", max=" + max);
        
        _whereBetween(columnName, min, max);
        
        return this;
    }
    
    /**
     * 指定の条件を作成しSQL文に追加します。
     * 
     * @param columnName カラム
     * @param min 下限
     * @param max 上限
     * @return オブジェクト
     */
    public SelectSqlBuilder whereBetween(String columnName, String min, String max) {
        
        Assert.isTrue(!StringUtils.isEmpty(min), "condition must not be null or empty.");
        Assert.isTrue(!StringUtils.isEmpty(max), "condition must not be null or empty.");
        
        _whereBetween(columnName, min, max);
        
        return this;
    }
    
    /**
     * 指定の条件を作成しSQL文に追加します。
     * 
     * @param columnName カラム
     * @param min 下限
     * @param max 上限
     */
    private void _whereBetween(String columnName, Object min, Object max) {
        
        Assert.isTrue(!StringUtils.isEmpty(columnName), "columnName must not be null or empty.");
        Assert.notNull(min, "condition must not be null.");
        Assert.notNull(max, "condition must not be null.");
        
        add(columnName).add("BETWEEN");
        add(":" + index.getValue());
        paramSource.addValue(index.getValue() + "", min);
        index.increment();
        add("AND");
        add(":" + index.getValue());
        paramSource.addValue(index.getValue() + "", max);
        index.increment();
    }
    
    
    
    /**
     * 指定の条件を作成しSQL文に追加します。
     * 
     * <p><code>type</code>に指定できる種別は<br>
     * {@link ConditionType#EQUAL}<br>
     * {@link ConditionType#NOT_EQUAL}<br>
     * {@link ConditionType#GREATER}<br>
     * {@link ConditionType#GREATER_OR_EQ}<br>
     * {@link ConditionType#LESS}<br>
     * {@link ConditionType#LESS_OR_EQ}<br>
     * のいずれかです。
     * 
     * <p><code>date</code>の'秒'以下の値は無効となります。
     * <code>date</code>の'秒'以下を切り捨てた値(<code>from</code>)と1分繰り上げた値(<code>to</code>)により、
     * 各種別に応じて以下のような条件を付与します。
     * 
     * <p>{@link ConditionType#EQUAL}を指定した場合
     * <pre>
     * (col >= to_timestamp(from, 'YYYY/MM/DD HH24:MI') and col < to_timestamp(to, 'YYYY/MM/DD HH24:MI'))
     * </pre>
     * 
     * <p>{@link ConditionType#NOT_EQUAL}を指定した場合
     * <pre>
     * ((col < to_timestamp(from, 'YYYY/MM/DD HH24:MI') or col >= to_timestamp(to, 'YYYY/MM/DD HH24:MI')))
     * </pre>
     * 
     * <p>{@link ConditionType#GREATER}を指定した場合
     * <pre>
     * col >= to_timestamp(to, 'YYYY/MM/DD HH24:MI')
     * </pre>
     * 
     * <p>{@link ConditionType#GREATER_OR_EQ}を指定した場合
     * <pre>
     * col >= to_timestamp(from, 'YYYY/MM/DD HH24:MI')
     * </pre>
     * 
     * <p>{@link ConditionType#LESS}を指定した場合
     * <pre>
     * col < to_timestamp(from, 'YYYY/MM/DD HH24:MI')
     * </pre>
     * 
     * <p>{@link ConditionType#LESS_OR_EQ}を指定した場合
     * <pre>
     * col <= to_timestamp(to, 'YYYY/MM/DD HH24:MI')
     * </pre>
     * 
     * 
     * <p>また、<code>null</code>(または空文字)を検索条件に指定したい場合は{@link #add(String)}を使用してください。
     * <pre>
     * 例)
     * add("col is null");
     * </pre>
     * 
     * @param columnName カラム
     * @param date 条件
     * @param type 検索条件種別
     * @return インスタンス
     */
    public SelectSqlBuilder whereDate(String columnName, java.util.Date date, ConditionType type) {
        
        Assert.notNull(date);
        
        // 秒以下の指定は無効
        
//        log.debug("date -> " + YY_MM_DD_HH_MI_SS.format(date));
        
        Date from = DateUtils.truncate(date, Calendar.MINUTE);  // 秒以下を切り捨てる
        
//        log.debug("from -> " + YY_MM_DD_HH_MI_SS.format(from));
        
        Date to = DateUtils.ceiling(date, Calendar.MINUTE);     // 分を繰り上げる
        if (to.getTime() > MAX_DATE_LONG) {
//            log.info("ToDate is over max.");
            to = new Date(MAX_DATE_LONG);
        }
        
//        log.debug("to -> " + YY_MM_DD_HH_MI_SS.format(to));
        
        whereDate(columnName, from, to, TO_DATE_FORMAT_YYMMDDHH24MI, YYMMDDHHMI.getFormatter(), type);
        
        return this;
    }
    
    /**
     * 指定の日時条件を作成しSQL文に追加します。
     * <p>{@link #whereDate(String, Date, ConditionType)}のJavaDocを参照のこと。
     * 
     * @param columnName カラム
     * @param year 年
     * @param month 月
     * @param day 日
     * @param hour 時
     * @param minute 分
     * @param type 検索条件種別
     * @return インスタンス
     */
    public SelectSqlBuilder whereDate(String columnName, int year, int month, int day, int hour, int minute, 
                                      ConditionType type) {
        
        Assert.isTrue(YYMMDDHHMI.isCorrect(new int[]{year, month, day, hour, minute}), "date is not correct. " + 
                "year -> " + year + ", month -> " + month + ", day -> " + day + ", hour -> " + hour + ", minute -> " + minute);
        
        Date date = YYMMDDHHMI.newDate(year, month, day, hour, minute);
        
//        log.debug("date -> " + YY_MM_DD_HH_MI_SS.format(date));
        
        return whereDate(columnName, date, type);
    }
    
    /**
     * 指定の日時条件を作成しSQL文に追加します。
     * <p>{@link #whereDate(String, Date, ConditionType)}のJavaDocを参照のこと。
     * 
     * @param columnName カラム
     * @param year 年
     * @param month 月
     * @param day 日
     * @param hour 時
     * @param minute 分
     * @param type 検索条件種別
     * @return インスタンス
     */
    public SelectSqlBuilder whereDate(String columnName, String year, String month, String day, String hour, String minute, 
                                      ConditionType type) {
        
        Assert.isTrue(YYMMDDHHMI.isCorrect(new String[]{year, month, day, hour, minute}), "date is not correct. " + 
                "year -> " + year + ", month -> " + month + ", day -> " + day + ", hour -> " + hour + ", minute -> " + minute);
        
        Date date = YYMMDDHHMI.newDate(year, month, day, hour, minute);
        
//        log.debug("date -> " + YY_MM_DD_HH_MI_SS.format(date));
        
        return whereDate(columnName, date, type);
    }
    
    /**
     * 指定の日時条件を作成しSQL文に追加します。
     * 
     * <p><code>type</code>に指定できる種別は<br>
     * {@link ConditionType#EQUAL}<br>
     * {@link ConditionType#NOT_EQUAL}<br>
     * {@link ConditionType#GREATER}<br>
     * {@link ConditionType#GREATER_OR_EQ}<br>
     * {@link ConditionType#LESS}<br>
     * {@link ConditionType#LESS_OR_EQ}<br>
     * のいずれかです。
     * 
     * <p>検索条件の'時'以下の値は無効となります。
     * 検索条件の'時'以下を切り捨てた値(<code>from</code>)と1日繰り上げた値(<code>to</code>)により、
     * 各種別に応じて以下のような条件を付与します。
     * 
     * <p>{@link ConditionType#EQUAL}を指定した場合
     * <pre>
     * (col >= to_timestamp(from, 'YYYY/MM/DD') and col < to_timestamp(to, 'YYYY/MM/DD'))
     * </pre>
     * 
     * <p>{@link ConditionType#NOT_EQUAL}を指定した場合
     * <pre>
     * ((col < to_timestamp(from, 'YYYY/MM/DD') or col >= to_timestamp(to, 'YYYY/MM/DD')))
     * </pre>
     * 
     * <p>{@link ConditionType#GREATER}を指定した場合
     * <pre>
     * col >= to_timestamp(to, 'YYYY/MM/DD')
     * </pre>
     * 
     * <p>{@link ConditionType#GREATER_OR_EQ}を指定した場合
     * <pre>
     * col >= to_timestamp(from, 'YYYY/MM/DD')
     * </pre>
     * 
     * <p>{@link ConditionType#LESS}を指定した場合
     * <pre>
     * col < to_timestamp(from, 'YYYY/MM/DD')
     * </pre>
     * 
     * <p>{@link ConditionType#LESS_OR_EQ}を指定した場合
     * <pre>
     * col < to_timestamp(to, 'YYYY/MM/DD')
     * </pre>
     * 
     * 
     * <p>また、<code>null</code>(または空文字)を検索条件に指定したい場合は{@link #add(String)}を使用してください。
     * <pre>
     * 例)
     * add("col is null");
     * </pre>
     * 
     * @param columnName カラム
     * @param year 年
     * @param month 月
     * @param day 日
     * @param type 検索条件種別
     * @return インスタンス
     */
    public SelectSqlBuilder whereDate(String columnName, int year, int month, int day, ConditionType type) {
        
        Assert.isTrue(YYMMDD.isCorrect(new int[]{year, month, day}), "date is not correct. " + 
                "year -> " + year + ", month -> " + month + ", day -> " + day);
        
//        log.debug("year -> " + year + ", month -> " + month + ", day -> " + day);
        
        Date from = YYMMDD.newDate(year, month, day);
        Date to = DateUtils.ceiling(from, Calendar.DATE);
        if (to.getTime() > MAX_DATE_LONG) {
            log.info("ToDate is over max.");
            to = new Date(MAX_DATE_LONG);
        }
        
//        log.debug("from -> " + YY_MM_DD_HH_MI_SS.format(from));
//        log.debug("to -> " + YY_MM_DD_HH_MI_SS.format(to));
        
        whereDate(columnName, from, to, TO_DATE_FORMAT_YYMMDD, YYMMDD.getFormatter(), type);
        
        return this;
    }
    
    /**
     * 指定の日時条件を作成しSQL文に追加します。
     * <p>{@link #whereDate(String, int, int, int, ConditionType)}のJavaDocを参照のこと。
     * 
     * @param columnName カラム
     * @param year 年
     * @param month 月
     * @param day 日
     * @param type 検索条件種別
     * @return インスタンス
     */
    public SelectSqlBuilder whereDate(String columnName, String year, String month, String day, ConditionType type) {
        
        Assert.isTrue(YYMMDD.isCorrect(new String[]{year, month, day}), "date is not correct. " + 
                "year -> " + year + ", month -> " + month + ", day -> " + day);
        
//        log.debug("year -> " + year + ", month -> " + month + ", day -> " + day);
        
        Date from = YYMMDD.newDate(year, month, day);
        Date to = DateUtils.ceiling(from, Calendar.DATE);
        if (to.getTime() > MAX_DATE_LONG) {
            log.info("ToDate is over max.");
            to = new Date(MAX_DATE_LONG);
        }
        
//        log.debug("from -> " + YY_MM_DD_HH_MI_SS.format(from));
//        log.debug("to -> " + YY_MM_DD_HH_MI_SS.format(to));
        
        whereDate(columnName, from, to, TO_DATE_FORMAT_YYMMDD, YYMMDD.getFormatter(), type);
        
        return this;
    }
    
    
    /**
     * 指定の日時条件を作成しSQL文に追加します。
     */
    private void whereDate(String columnName, java.util.Date from, java.util.Date to, 
                           String format, FastDateFormat formatter, ConditionType type) {
        
        Assert.isTrue(!StringUtils.isEmpty(columnName), "columnName must not be null or empty.");
        Assert.notNull(from);
        Assert.notNull(to);
        Assert.notNull(format);
        Assert.notNull(formatter);
        Assert.notNull(type);
        Assert.isTrue(type.equals(ConditionType.EQUAL)
                || type.equals(ConditionType.NOT_EQUAL)
                || type.equals(ConditionType.GREATER)
                || type.equals(ConditionType.GREATER_OR_EQ)
                || type.equals(ConditionType.LESS)
                || type.equals(ConditionType.LESS_OR_EQ));
        
        switch (type) {
        case EQUAL:
            // (col >= to_timestamp(from, FORMAT)
            // and
            // col < to_timestamp(to, FORMAT))
            
            add("(");
            add(columnName);
            add(">=").add("TO_TIMESTAMP(").add(":" + index.getValue()).add(", '"+format+"')");
            paramSource.addValue(index.getValue() + "", formatter.format(from));
            index.increment();
            add("AND");
            add(columnName);
            add("<").add("TO_TIMESTAMP(").add(":" + index.getValue()).add(", '"+format+"')");
            paramSource.addValue(index.getValue() + "", formatter.format(to));
            index.increment();
            add(")");
            
            break;
            
        case NOT_EQUAL:
            // (col < to_timestamp(from, FORMAT)
            // or
            // col >= to_timestamp(to, FORMAT))
            
            add("(");
            add(columnName);
            add("<").add("TO_TIMESTAMP(").add(":" + index.getValue()).add(", '"+format+"')");
            paramSource.addValue(index.getValue() + "", formatter.format(from));
            index.increment();
            add("OR");
            add(columnName);
            add(">=").add("TO_TIMESTAMP(").add(":" + index.getValue()).add(", '"+format+"')");
            paramSource.addValue(index.getValue() + "", formatter.format(to));
            index.increment();
            add(")");
            
            break;
            
        case GREATER:
            // col >= to_timestamp(to, FORMAT)
            
            add(columnName);
            add(">=").add("TO_TIMESTAMP(").add(":" + index.getValue()).add(", '"+format+"')");
            paramSource.addValue(index.getValue() + "", formatter.format(to));
            index.increment();
            
            break;
            
        case GREATER_OR_EQ:
            // col >= to_timestamp(from, FORMAT)
            
            add(columnName);
            add(">=").add("TO_TIMESTAMP(").add(":" + index.getValue()).add(", '"+format+"')");
            paramSource.addValue(index.getValue() + "", formatter.format(from));
            index.increment();
            
            break;
            
        case LESS:
            // col < to_timestamp(from, FORMAT)
            
            add(columnName);
            add("<").add("TO_TIMESTAMP(").add(":" + index.getValue()).add(", '"+format+"')");
            paramSource.addValue(index.getValue() + "", formatter.format(to));
            index.increment();
            
            break;
            
        case LESS_OR_EQ:
            // col <= to_timestamp(to, FORMAT)
            
            add(columnName);
            add("<=").add("TO_TIMESTAMP(").add(":" + index.getValue()).add(", '"+format+"')");
            paramSource.addValue(index.getValue() + "", formatter.format(to));
            index.increment();
            
            break;
            
        default:
            // 上でチェックしているのでここにくることはありえない
            break;
        }
    }
    
    
    /**
     * 初期日時を除く条件を作成しSQL文に追加します。
     * <p>以下のような条件を付与します。
     * <pre>
     * col != to_date('0001/01/01 00:00:00', 'YYYY/MM/DD HH24:MI:SS')
     * </pre>
     * 
     * @param columnName カラム
     * @return オブジェクト
     */
    public SelectSqlBuilder whereNotEqInitalDate(String columnName) {
        
        Assert.isTrue(!StringUtils.isEmpty(columnName), "columnName must not be null or empty.");
        
        add(columnName);
        add("!=").add("TO_DATE('"+INITIAL_DATE_STRING+"', 'YYYY/MM/DD')");
        
        return this;
    }
    
    
    /**
     * 指定の条件を作成しSQL文に追加します。
     * 
     * <p><code>fromDate</code>、<code>toDate</code>の'秒'以下の値は無効となります。
     * <code>fromDate</code>の'秒'以下を切り捨てた値(<code>from</code>)と
     * <code>toDate</code>を1分繰り上げた値(<code>to</code>)により、
     * 以下のような条件を付与します。
     * 
     * <pre>
     * (col >= to_timestamp(from, 'YYYY/MM/DD HH24:MI') and col < to_timestamp(to, 'YYYY/MM/DD HH24:MI'))
     * </pre>
     * 
     * @param columnName カラム
     * @param fromDate from日時
     * @param toDate to日時
     * @return オブジェクト
     */
    public SelectSqlBuilder whereBetweenDate(String columnName, java.util.Date fromDate, java.util.Date toDate) {
        
        Assert.notNull(fromDate, "fromDate must not be null.");
        Assert.notNull(toDate, "toDate must not be null.");
        Assert.isTrue(fromDate.getTime() <= toDate.getTime(), "fromDate must be less or equals toDate.");
        
        // 秒以下の指定は無効
        
        log.debug("fromDate -> " + YY_MM_DD_HH_MI_SS.format(fromDate));
        
        Date from = DateUtils.truncate(fromDate, Calendar.MINUTE);  // 秒以下を切り捨てる
        
//        log.debug("from -> " + YY_MM_DD_HH_MI_SS.format(from));
        
//        log.debug("toDate -> " + YY_MM_DD_HH_MI_SS.format(toDate));
        
        Date to = DateUtils.ceiling(toDate, Calendar.MINUTE);     // 分を繰り上げる
        if (to.getTime() > MAX_DATE_LONG) {
            log.info("ToDate is over max.");
            to = new Date(MAX_DATE_LONG);
        }
        
//        log.debug("to -> " + YY_MM_DD_HH_MI_SS.format(to));
        
        whereDate(columnName, from, to, TO_DATE_FORMAT_YYMMDDHH24MI, YYMMDDHHMI.getFormatter(), ConditionType.EQUAL);
        
        return this;
    }
    
    /**
     * 指定の条件を作成しSQL文に追加します。
     * <p>{@link #whereBetweenDate(String, Date, Date)}のJavaDocを参照のこと。
     * 
     * @param columnName カラム
     * @param fromYear from年
     * @param fromMonth from月
     * @param fromDay from日
     * @param fromHour from時
     * @param fromMinute from分
     * @param toYear to年
     * @param toMonth to月
     * @param toDay to日
     * @param toHour to時
     * @param toMinute to分
     * @return オブジェクト
     */
    public SelectSqlBuilder whereBetweenDate(String columnName, 
                                             int fromYear, int fromMonth, int fromDay, int fromHour, int fromMinute, 
                                             int toYear, int toMonth, int toDay, int toHour, int toMinute) {
        
        Assert.isTrue(YYMMDDHHMI.isCorrect(new int[]{fromYear, fromMonth, fromDay, fromHour, fromMinute}), "fromDate is not correct. " + 
                "fromYear -> " + fromYear + ", fromMonth -> " + fromMonth + ",  fromDay -> " + fromDay + 
                ", fromHour -> " + fromHour + ", fromMinute -> " + fromMinute);
        
        Assert.isTrue(YYMMDDHHMI.isCorrect(new int[]{toYear, toMonth, toDay, toHour, toMinute}), "toDate is not correct. " + 
                "toYear -> " + toYear + ", toMonth -> " + toMonth + ", toDay -> " + toDay + 
                ", toHour -> " + toHour + ", toMinute -> " + toMinute);
        
//        log.debug("fromYear -> " + fromYear + ", fromMonth -> " + fromMonth + ",  fromDay -> " + fromDay + 
//                ", fromHour -> " + fromHour + ", fromMinute -> " + fromMinute);
        
//        log.debug("toYear -> " + toYear + ", toMonth -> " + toMonth + ", toDay -> " + toDay + 
//                ", toHour -> " + toHour + ", toMinute -> " + toMinute);
        
        Date fromDate = YYMMDDHHMI.newDate(fromYear, fromMonth, fromDay, fromHour, fromMinute);
        Date toDate = YYMMDDHHMI.newDate(toYear, toMonth, toDay, toHour, toMinute);
        
        return whereBetweenDate(columnName, fromDate, toDate);
    }
    
    /**
     * 指定の条件を作成しSQL文に追加します。
     * <p>{@link #whereBetweenDate(String, Date, Date)}のJavaDocを参照のこと。
     * 
     * @param columnName カラム
     * @param fromYear from年
     * @param fromMonth from月
     * @param fromDay from日
     * @param fromHour from時
     * @param fromMinute from分
     * @param toYear to年
     * @param toMonth to月
     * @param toDay to日
     * @param toHour to時
     * @param toMinute to分
     * @return オブジェクト
     */
    public SelectSqlBuilder whereBetweenDate(String columnName, 
                                             String fromYear, String fromMonth, String fromDay, 
                                             String fromHour, String fromMinute, 
                                             String toYear, String toMonth, String toDay, 
                                             String toHour, String toMinute) {
        
        Assert.isTrue(YYMMDDHHMI.isCorrect(new String[]{fromYear, fromMonth, fromDay, fromHour, fromMinute}), "fromDate is not correct. " + 
                "fromYear -> " + fromYear + ", fromMonth -> " + fromMonth + ",  fromDay -> " + fromDay + 
                ", fromHour -> " + fromHour + ", fromMinute -> " + fromMinute);
        
        Assert.isTrue(YYMMDDHHMI.isCorrect(new String[]{toYear, toMonth, toDay, toHour, toMinute}), "toDate is not correct. " + 
                "toYear -> " + toYear + ", toMonth -> " + toMonth + ", toDay -> " + toDay + 
                ", toHour -> " + toHour + ", toMinute -> " + toMinute);
        
//        log.debug("fromYear -> " + fromYear + ", fromMonth -> " + fromMonth + ",  fromDay -> " + fromDay + 
//                ", fromHour -> " + fromHour + ", fromMinute -> " + fromMinute);
        
//        log.debug("toYear -> " + toYear + ", toMonth -> " + toMonth + ", toDay -> " + toDay + 
//                ", toHour -> " + toHour + ", toMinute -> " + toMinute);
        
        Date fromDate = YYMMDDHHMI.newDate(fromYear, fromMonth, fromDay, fromHour, fromMinute);
        Date toDate = YYMMDDHHMI.newDate(toYear, toMonth, toDay, toHour, toMinute);
        
        return whereBetweenDate(columnName, fromDate, toDate);
    }
    
    
    /**
     * 指定の条件を作成しSQL文に追加します。
     * 
     * <p>検索条件の'時'以下の値は無効となります。
     * 検索条件の'時'以下を切り捨てた値(<code>from</code>)と1日繰り上げた値(<code>to</code>)により、
     * 以下のような条件を付与します。
     * 
     * <p>{@link ConditionType#EQUAL}を指定した場合
     * <pre>
     * (col >= to_timestamp(from, 'YYYY/MM/DD') and col < to_timestamp(to, 'YYYY/MM/DD'))
     * </pre>
     * 
     * @param columnName カラム
     * @param fromYear from年
     * @param fromMonth from月
     * @param fromDay from日
     * @param toYear to年
     * @param toMonth to月
     * @param toDay to日
     * @return オブジェクト
     */
    public SelectSqlBuilder whereBetweenDate(String columnName, 
                                             int fromYear, int fromMonth, int fromDay, 
                                             int toYear, int toMonth, int toDay) {
        
        Assert.isTrue(YYMMDD.isCorrect(new int[]{fromYear, fromMonth, fromDay}), "date is not correct. " + 
                "fromYear -> " + fromYear + ", fromMonth -> " + fromMonth + ", fromDay -> " + fromDay);
        
        Assert.isTrue(YYMMDD.isCorrect(new int[]{toYear, toMonth, toDay}), "date is not correct. " + 
                "toYear -> " + toYear + ", toMonth -> " + toMonth + ", toDay -> " + toDay);
        
//        log.debug("fromYear -> " + fromYear + ", fromMonth -> " + fromMonth + ", fromDay -> " + fromDay);
        
//        log.debug("toYear -> " + toYear + ", toMonth -> " + toMonth + ", toDay -> " + toDay);
        
        Date from = YYMMDD.newDate(fromYear, fromMonth, fromDay);
        Date to = YYMMDDHHMI.newDate(toYear, toMonth, toDay, 23, 59);
        to = DateUtils.round(to, Calendar.DAY_OF_MONTH);
        
//        log.debug("fromDate -> " + YYMMDDHHMI.format(from));
//        log.debug("toDate -> " + YYMMDDHHMI.format(to));
        
        whereDate(columnName, from, to, TO_DATE_FORMAT_YYMMDD, YYMMDD.getFormatter(), ConditionType.EQUAL);
        
        return this;
    }
    
    /**
     * 指定の条件を作成しSQL文に追加します。
     * <p>{@link #whereBetweenDate(String, int, int, int, int, int, int)}のJavaDocを参照のこと。
     * 
     * @param columnName カラム
     * @param fromYear from年
     * @param fromMonth from月
     * @param fromDay from日
     * @param toYear to年
     * @param toMonth to月
     * @param toDay to日
     * @return オブジェクト
     */
    public SelectSqlBuilder whereBetweenDate(String columnName, 
                                             String fromYear, String fromMonth, String fromDay, 
                                             String toYear, String toMonth, String toDay) {
        
        Assert.isTrue(YYMMDD.isCorrect(new String[]{fromYear, fromMonth, fromDay}), "date is not correct. " + 
                "fromYear -> " + fromYear + ", fromMonth -> " + fromMonth + ", fromDay -> " + fromDay);
        
        Assert.isTrue(YYMMDD.isCorrect(new String[]{toYear, toMonth, toDay}), "date is not correct. " + 
                "toYear -> " + toYear + ", toMonth -> " + toMonth + ", toDay -> " + toDay);
        
//        log.debug("fromYear -> " + fromYear + ", fromMonth -> " + fromMonth + ", fromDay -> " + fromDay);
        
//        log.debug("toYear -> " + toYear + ", toMonth -> " + toMonth + ", toDay -> " + toDay);
        
        Date from = YYMMDD.newDate(fromYear, fromMonth, fromDay);
        Date to = YYMMDDHHMI.newDate(toYear, toMonth, toDay, "23", "59");
        to = DateUtils.round(to, Calendar.DAY_OF_MONTH);
        
//        log.debug("fromDate -> " + YYMMDDHHMI.format(from));
//        log.debug("toDate -> " + YYMMDDHHMI.format(to));
        
        whereDate(columnName, from, to, TO_DATE_FORMAT_YYMMDD, YYMMDD.getFormatter(), ConditionType.EQUAL);
        
        return this;
    }
    
    
    /**
     * コンストラクタ。
     */
    private SelectSqlBuilder() {    // インスタンス化不可
        buffer = new StringBuilder();
        paramSource = new MapSqlParameterSource();
        index = new ParameterIndex();
    }
    
    /** SQL文のバッファ。 */
    private StringBuilder buffer;
    
    /** SQLパラメータ */
    private MapSqlParameterSource paramSource;
    
    /** インデックス(SQLパラメータのキー) */
    private ParameterIndex index;
    
    /** ログ。 */
    private final static Log log = LogFactory.getLog(SelectSqlBuilder.class);
    
    private static final boolean isDebugMode;
    static {
        isDebugMode = System.getProperty("debugMode") != null;
    }
    
    /**
     * SQL文を返します。
     * 
     * @return SQL文
     */
    public String getSql() {
        if (isDebugMode) {
            log.info(buffer.toString());
        }
        return buffer.toString();
    }
    
    /**
     * SQLパラメータを返します。
     * 
     * @return SQLのパラメータ
     */
    public SqlParameterSource getSqlParameterSource() {
        if (isDebugMode) {
            log.info(paramSource.getValues().toString());
        }
        return paramSource;
    }
    
    /**
     * SQL文を返します。
     * 
     * @param ignoreLog SQL文をログ出力しない場合<code>true</code>
     * @return SQL文
     */
    public String getSql(boolean ignoreLog) {
        if (ignoreLog) {
            return buffer.toString();
        } else {
            return getSql();
        }
    }
    
    /**
     * SQLパラメータを返します。
     * 
     * @param ignoreLog SQLのパラメータをログ出力しない場合<code>true</code>
     * @return SQLのパラメータ
     */
    public SqlParameterSource getSqlParameterSource(boolean ignoreLog) {
        if (ignoreLog) {
            return paramSource;
        } else {
            return getSqlParameterSource();
        }
    }
    
    /** to_Date関数の日時フォーマット：yyyyMMdd。 */
    public final static String TO_DATE_FORMAT_YYMMDD = "yyyyMMdd";
    
    /** to_Date関数の日時フォーマット：yyyyMMddhh24mi。 */
    public final static String TO_DATE_FORMAT_YYMMDDHH24MI = "yyyyMMddhh24mi";
    
    /**
     * 検索するカラムの検索条件種別を定義したenumです。
     * 
     * @author  kurisakisatoshi
     * @version $Revision: 443 $
     */
    public enum ConditionType {
        
        /** 前方一致 */
        Prefix {
            
            /**
             * {@inheritDoc}
             */
            @Override
            String makeCondition(String column, String cond) {
                return column + " LIKE " + cond + "|| '%' ESCAPE '\\'";
            }
        }, 
        
        /** 後方一致 */
        Suffix {
            
            /**
             * {@inheritDoc}
             */
            @Override
            String makeCondition(String column, String cond) {
                return column + " LIKE '%' || " + cond + "ESCAPE '\\'";
            }
        }, 
        
        /** 部分一致 */
        Partial {
            
            /**
             * {@inheritDoc}
             */
            @Override
            String makeCondition(String column, String cond) {
                return column + " LIKE '%' || " + cond + " || '%' ESCAPE '\\'";
            }
        }, 
        
        /** 完全一致(=) */
        EQUAL {
            
            /**
             * {@inheritDoc}
             */
            @Override
            String makeCondition(String column, String cond) {
                return column + " = " + cond;
            }
            
            /**
             * {@inheritDoc}
             */
            @Override
            String escape(String before) {
                return before;
            }
        }, 
        
        /** 完全一致の否定系(!=) */
        NOT_EQUAL {
            
            /**
             * {@inheritDoc}
             */
            @Override
            String makeCondition(String column, String cond) {
                return column + " != " + cond;
            }
            
            /**
             * {@inheritDoc}
             */
            @Override
            String escape(String before) {
                return before;
            }
        }, 
        
        /** 大なり(＞) */
        GREATER {
            
            /**
             * {@inheritDoc}
             */
            @Override
            String makeCondition(String column, String cond) {
                return column + " > " + cond;
            }
            
            /**
             * {@inheritDoc}
             */
            @Override
            String escape(String before) {
                return before;
            }
        }, 
        
        /** 少なり(＜) */
        LESS {
            
            /**
             * {@inheritDoc}
             */
            @Override
            String makeCondition(String column, String cond) {
                return column + " < " + cond;
            }
            
            /**
             * {@inheritDoc}
             */
            @Override
            String escape(String before) {
                return before;
            }
        }, 
        
        /** 以上(＞=) */
        GREATER_OR_EQ {
            
            /**
             * {@inheritDoc}
             */
            @Override
            String makeCondition(String column, String cond) {
                return column + " >= " + cond;
            }
            
            /**
             * {@inheritDoc}
             */
            @Override
            String escape(String before) {
                return before;
            }
        },
        
        /** 以下(＜=) */
        LESS_OR_EQ {
            
            /**
             * {@inheritDoc}
             */
            @Override
            String makeCondition(String column, String cond) {
                return column + " <= " + cond;
            }
            
            /**
             * {@inheritDoc}
             */
            @Override
            String escape(String before) {
                return before;
            }
        }, 
        ;
        
        /**
         * 検索条件を作成します。
         * 
         * @param column カラム名
         * @param cond 条件
         * @return 検索条件
         */
        abstract String makeCondition(String column, String cond);
        
        /**
         * ワイルドカード文字をエスケープします。
         * 
         * @param before エスケープ前
         * @return エスケープ後
         */
        String escape(String before) {
            StringBuilder after = new StringBuilder();
            
            for (char c : before.toCharArray()) {
                switch (c) {
                case '%':
                case '_':
                case '％':
                case '＿':
                case '\\':
                    after.append("\\");
                    break;
                default:
                    break;
                }
                
                after.append(c);
            }
            
            return after.toString();
        }
    }
}
