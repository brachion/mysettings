
import java.text.ParseException;
import java.util.Calendar;
import java.util.Date;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.math.NumberUtils;
import org.apache.commons.lang.time.DateUtils;
import org.apache.commons.lang.time.FastDateFormat;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.util.Assert;

/**
 * 日付関連のユーティリティクラスです。
 * <p>
 *
 * @author  kurisakisatoshi
 * @version $Revision: 429 $
 */
public enum DateUtil {

    /** yyyyMMddの形式。 */
    YYMMDD("yyyyMMdd") {

        /**
         * {@inheritDoc}
         */
        @Override
        public Date newDate(int... params) {

            Assert.isTrue(params.length == 3, "params.length must be 3. year, month and day.");

            int year = params[0];
            int month = params[1];
            int day = params[2];

            getLog().debug("year -> " + year + ", month -> " + month + ", day -> " + day);

            Date date = new Date(INITIAL_DATE.getTime());
            date = DateUtils.setYears(date, year);
            date = DateUtils.setMonths(date, month-1);
            date = DateUtils.setDays(date, day);
            date = DateUtils.truncate(date, Calendar.DAY_OF_MONTH); // 時以降は切り捨て

            getLog().debug("date -> " + YY_MM_DD_HH_MI_SS.format(date));

            return date;
        }

        /**
         * {@inheritDoc}
         */
        @Override
        String toDateString(String... params) {

            Assert.isTrue(params.length == 3, "params.length must be 3. year, month and day.");

            StringBuilder sb = new StringBuilder();

            sb.append(StringUtils.leftPad(params[0], 4, "0"));
            sb.append(StringUtils.leftPad(params[1], 2, "0"));
            sb.append(StringUtils.leftPad(params[2], 2, "0"));

            return sb.toString();
        }
        
        /**
         * {@inheritDoc}
         */
        @Override
        public boolean isCorrect(int... params) {
            return YY_MM_DD.isCorrect(params);
        }
        
        /**
         * {@inheritDoc}
         */
        @Override
        public boolean isCorrect(String... params) {
            return YY_MM_DD.isCorrect(params);
        }
    },

    /** yyyyMMddHHの形式。 */
    YYMMDDHH("yyyyMMddHH") {

        /**
         * {@inheritDoc}
         */
        @Override
        public Date newDate(int... params) {

            Assert.isTrue(params.length == 4, "params.length must be 4. year, month, day and hour.");

            int year = params[0];
            int month = params[1];
            int day = params[2];
            int hour = params[3];

            getLog().debug("year -> " + year + ", month -> " + month + ", day -> " + day +
                    ", hour -> " + hour);

            Date date = new Date(INITIAL_DATE.getTime());
            date = DateUtils.setYears(date, year);
            date = DateUtils.setMonths(date, month-1);
            date = DateUtils.setDays(date, day);
            date = DateUtils.setHours(date, hour);
            date = DateUtils.truncate(date, Calendar.MINUTE); // 秒以降は切り捨て

            getLog().debug("date -> " + YY_MM_DD_HH_MI_SS.format(date));

            return date;
        }

        /**
         * {@inheritDoc}
         */
        @Override
        String toDateString(String... params) {

            Assert.isTrue(params.length == 4, "params.length must be 4. year, month, day and hour.");

            StringBuilder sb = new StringBuilder();

            sb.append(StringUtils.leftPad(params[0], 4, "0"));
            sb.append(StringUtils.leftPad(params[1], 2, "0"));
            sb.append(StringUtils.leftPad(params[2], 2, "0"));
            sb.append(StringUtils.leftPad(params[3], 2, "0"));

            return sb.toString();
        }
        
        /**
         * {@inheritDoc}
         */
        @Override
        public boolean isCorrect(int... params) {
            return YY_MM_DD_HH.isCorrect(params);
        }
        
        /**
         * {@inheritDoc}
         */
        @Override
        public boolean isCorrect(String... params) {
            return YY_MM_DD_HH.isCorrect(params);
        }
    },

    /** yyyyMMddHHmmの形式。 */
    YYMMDDHHMI("yyyyMMddHHmm") {

        /**
         * {@inheritDoc}
         */
        @Override
        public Date newDate(int... params) {

            Assert.isTrue(params.length == 5, "params.length must be 5. year, month, day, hour and minute.");

            int year = params[0];
            int month = params[1];
            int day = params[2];
            int hour = params[3];
            int minute = params[4];

            getLog().debug("year -> " + year + ", month -> " + month + ", day -> " + day +
                    ", hour -> " + hour + ", minute -> " + minute);

            Date date = new Date(INITIAL_DATE.getTime());
            date = DateUtils.setYears(date, year);
            date = DateUtils.setMonths(date, month-1);
            date = DateUtils.setDays(date, day);
            date = DateUtils.setHours(date, hour);
            date = DateUtils.setMinutes(date, minute);
            date = DateUtils.truncate(date, Calendar.MINUTE); // 秒以降は切り捨て

            getLog().debug("date -> " + YY_MM_DD_HH_MI_SS.format(date));

            return date;
        }

        /**
         * {@inheritDoc}
         */
        @Override
        String toDateString(String... params) {

            Assert.isTrue(params.length == 5, "params.length must be 5. year, month, day, hour and minute.");

            StringBuilder sb = new StringBuilder();

            sb.append(StringUtils.leftPad(params[0], 4, "0"));
            sb.append(StringUtils.leftPad(params[1], 2, "0"));
            sb.append(StringUtils.leftPad(params[2], 2, "0"));
            sb.append(StringUtils.leftPad(params[3], 2, "0"));
            sb.append(StringUtils.leftPad(params[4], 2, "0"));

            return sb.toString();
        }
        
        /**
         * {@inheritDoc}
         */
        @Override
        public boolean isCorrect(int... params) {
            return YY_MM_DD_HH_MI.isCorrect(params);
        }
        
        /**
         * {@inheritDoc}
         */
        @Override
        public boolean isCorrect(String... params) {
            return YY_MM_DD_HH_MI.isCorrect(params);
        }
    },

    /** yyyyMMddHHmmssの形式。 */
    YYMMDDHHMISS("yyyyMMddHHmmss") {

        /**
         * {@inheritDoc}
         */
        @Override
        public Date newDate(int... params) {

            Assert.isTrue(params.length == 6, "params.length must be 6. year, month, day, hour, minute and second.");

            int year = params[0];
            int month = params[1];
            int day = params[2];
            int hour = params[3];
            int minute = params[4];
            int second = params[5];

            getLog().debug("year -> " + year + ", month -> " + month + ", day -> " + day +
                    ", hour -> " + hour + ", minute -> " + minute + ", second -> " + second);

            Date date = new Date(INITIAL_DATE.getTime());
            date = DateUtils.setYears(date, year);
            date = DateUtils.setMonths(date, month-1);
            date = DateUtils.setDays(date, day);
            date = DateUtils.setHours(date, hour);
            date = DateUtils.setMinutes(date, minute);
            date = DateUtils.setSeconds(date, second);

            getLog().debug("date -> " + YY_MM_DD_HH_MI_SS.format(date));

            return date;
        }

        /**
         * {@inheritDoc}
         */
        @Override
        String toDateString(String... params) {

            Assert.isTrue(params.length == 6, "params.length must be 6. year, month, day, hour, minute and second.");

            StringBuilder sb = new StringBuilder();

            sb.append(StringUtils.leftPad(params[0], 4, "0"));
            sb.append(StringUtils.leftPad(params[1], 2, "0"));
            sb.append(StringUtils.leftPad(params[2], 2, "0"));
            sb.append(StringUtils.leftPad(params[3], 2, "0"));
            sb.append(StringUtils.leftPad(params[4], 2, "0"));
            sb.append(StringUtils.leftPad(params[5], 2, "0"));

            return sb.toString();
        }
        
        /**
         * {@inheritDoc}
         */
        @Override
        public boolean isCorrect(int... params) {
            return YY_MM_DD_HH_MI_SS.isCorrect(params);
        }
        
        /**
         * {@inheritDoc}
         */
        @Override
        public boolean isCorrect(String... params) {
            return YY_MM_DD_HH_MI_SS.isCorrect(params);
        }
    },

    /** yyyy/MM/ddの形式。 */
    YY_MM_DD("yyyy/MM/dd") {

        /**
         * {@inheritDoc}
         */
        @Override
        public Date newDate(int... params) {
            return YYMMDD.newDate(params);
        }

        /**
         * {@inheritDoc}
         */
        @Override
        String toDateString(String... params) {

            Assert.isTrue(params.length == 3, "params.length must be 3. year, month and day.");

            StringBuilder sb = new StringBuilder();

            sb.append(StringUtils.leftPad(params[0], 4, "0")).append("/");
            sb.append(StringUtils.leftPad(params[1], 2, "0")).append("/");
            sb.append(StringUtils.leftPad(params[2], 2, "0"));

            return sb.toString();
        }
    },

    /** yyyy/MM/dd HHの形式 */
    YY_MM_DD_HH("yyyy/MM/dd HH") {

        /**
         * {@inheritDoc}
         */
        @Override
        public Date newDate(int... params) {
            return YYMMDDHH.newDate(params);
        }

        /**
         * {@inheritDoc}
         */
        @Override
        String toDateString(String... params) {

            Assert.isTrue(params.length == 4, "params.length must be 4. year, month, day and hour.");

            StringBuilder sb = new StringBuilder();

            sb.append(StringUtils.leftPad(params[0], 4, "0")).append("/");
            sb.append(StringUtils.leftPad(params[1], 2, "0")).append("/");
            sb.append(StringUtils.leftPad(params[2], 2, "0")).append(" ");
            sb.append(StringUtils.leftPad(params[3], 2, "0"));

            return sb.toString();
        }
    },

    /** yyyy/MM/dd HH:mmの形式。 */
    YY_MM_DD_HH_MI("yyyy/MM/dd HH:mm") {

        /**
         * {@inheritDoc}
         */
        @Override
        public Date newDate(int... params) {
            return YYMMDDHHMI.newDate(params);
        }

        /**
         * {@inheritDoc}
         */
        @Override
        String toDateString(String... params) {

            Assert.isTrue(params.length == 5, "params.length must be 5. year, month, day, hour and minute.");

            StringBuilder sb = new StringBuilder();

            sb.append(StringUtils.leftPad(params[0], 4, "0")).append("/");
            sb.append(StringUtils.leftPad(params[1], 2, "0")).append("/");
            sb.append(StringUtils.leftPad(params[2], 2, "0")).append(" ");
            sb.append(StringUtils.leftPad(params[3], 2, "0")).append(":");
            sb.append(StringUtils.leftPad(params[4], 2, "0"));

            return sb.toString();
        }
    },

    /** yyyy/MM/dd HH:mm:SSの形式。 */
    YY_MM_DD_HH_MI_SS("yyyy/MM/dd HH:mm:ss") {

        /**
         * {@inheritDoc}
         */
        @Override
        public Date newDate(int... params) {
            return YYMMDDHHMISS.newDate(params);
        }

        /**
         * {@inheritDoc}
         */
        @Override
        String toDateString(String... params) {

            Assert.isTrue(params.length == 6, "params.length must be 6. year, month, day, hour, minute and second.");

            StringBuilder sb = new StringBuilder();

            sb.append(StringUtils.leftPad(params[0], 4, "0")).append("/");
            sb.append(StringUtils.leftPad(params[1], 2, "0")).append("/");
            sb.append(StringUtils.leftPad(params[2], 2, "0")).append(" ");
            sb.append(StringUtils.leftPad(params[3], 2, "0")).append(":");
            sb.append(StringUtils.leftPad(params[4], 2, "0")).append(":");
            sb.append(StringUtils.leftPad(params[5], 2, "0"));

            return sb.toString();
        }
    },

    /** HHmmの形式。 */
    HHMI("HHmm") {

        /**
         * {@inheritDoc}
         */
        @Override
        public Date newDate(int... params) {
            throw new UnsupportedOperationException("この操作はサポートしていません。");
        }

        /**
         * {@inheritDoc}
         */
        @Override
        String toDateString(String... params) {

            Assert.isTrue(params.length == 2, "params.length must be 2. hour and minute.");

            StringBuilder sb = new StringBuilder();

            sb.append(StringUtils.leftPad(params[0], 2, "0"));
            sb.append(StringUtils.leftPad(params[1], 2, "0"));

            return sb.toString();
        }
        
        /**
         * {@inheritDoc}
         */
        @Override
        public boolean isCorrect(int... params) {
            return HH_MI.isCorrect(params);
        }
        
        /**
         * {@inheritDoc}
         */
        @Override
        public boolean isCorrect(String... params) {
            return HH_MI.isCorrect(params);
        }
    },

    /** HHmmSSの形式。 */
    HHMISS("HHmmss") {

        /**
         * {@inheritDoc}
         */
        @Override
        public Date newDate(int... params) {
            throw new UnsupportedOperationException("この操作はサポートしていません。");
        }

        /**
         * {@inheritDoc}
         */
        @Override
        String toDateString(String... params) {

            Assert.isTrue(params.length == 3, "params.length must be 3. hour, minute and second.");

            StringBuilder sb = new StringBuilder();

            sb.append(StringUtils.leftPad(params[0], 2, "0"));
            sb.append(StringUtils.leftPad(params[1], 2, "0"));
            sb.append(StringUtils.leftPad(params[2], 2, "0"));

            return sb.toString();
        }
        
        /**
         * {@inheritDoc}
         */
        @Override
        public boolean isCorrect(int... params) {
            return HH_MI_SS.isCorrect(params);
        }
        
        /**
         * {@inheritDoc}
         */
        @Override
        public boolean isCorrect(String... params) {
            return HH_MI_SS.isCorrect(params);
        }
    },

    /** HH:mmの形式。 */
    HH_MI("HH:mm") {

        /**
         * {@inheritDoc}
         */
        @Override
        public Date newDate(int... params) {
            throw new UnsupportedOperationException("この操作はサポートしていません。");
        }

        /**
         * {@inheritDoc}
         */
        @Override
        String toDateString(String... params) {

            Assert.isTrue(params.length == 2, "params.length must be 2. hour and minute.");

            StringBuilder sb = new StringBuilder();

            sb.append(StringUtils.leftPad(params[0], 2, "0")).append(":");
            sb.append(StringUtils.leftPad(params[1], 2, "0"));

            return sb.toString();
        }
    },

    /** HH:mm:SSの形式。 */
    HH_MI_SS("HH:mm:ss") {

        /**
         * {@inheritDoc}
         */
        @Override
        public Date newDate(int... params) {
            throw new UnsupportedOperationException("この操作はサポートしていません。");
        }

        /**
         * {@inheritDoc}
         */
        @Override
        String toDateString(String... params) {

            Assert.isTrue(params.length == 3, "params.length must be 3. hour, minute and second.");

            StringBuilder sb = new StringBuilder();

            sb.append(StringUtils.leftPad(params[0], 2, "0")).append(":");
            sb.append(StringUtils.leftPad(params[1], 2, "0")).append(":");
            sb.append(StringUtils.leftPad(params[2], 2, "0"));

            return sb.toString();
        }
    },
    ;

    /** 変換フォーマット。 */
    private final String format;

    /** 変換フォーマッタ。 */
    private final FastDateFormat formatter;

    /**
     * コンストラクタ。
     *
     * @param format フォーマット
     */
    private DateUtil(String format) {
        this.format = format;
        this.formatter = FastDateFormat.getInstance(format);
    }

    /**
     * 日時のフォーマットを返します。
     *
     * @return 日時のフォーマット
     */
    public String getFormat() {
        return format;
    }

    /**
     * 日時のフォーマッタを返します。
     *
     * @return 日時のフォーマッタ
     */
    public FastDateFormat getFormatter() {
        return formatter;
    }

    /**
     * <code>date</code>オブジェクトを文字列形式にフォーマットします。
     *
     * @param date フォーマット対象のオブジェクト
     * @return 文字列形式の日時
     */
    public String format(Date date) {

        if (date == null) {
            getLog().debug("date is null.");
            return "";
        }

        if (DateUtils.isSameDay(date, getInitialDate())) {
            getLog().debug("date is empty. -> " + date);
            return "";
        }

        return formatter.format(date);
    }

    /**
     * 指定の<code>params</code>が日時の情報として適切な場合<code>true</code>を返します。
     * 例えば、2011年2月30日などは<code>false</code>となります。
     *
     * <p><code>params</code>にはフォーマッタに応じて年月日などの順に格納されている必要があります。
     *
     * @param params 日時の情報
     * @return 日時として適切な場合、<code>true</code>
     */
    public boolean isCorrect(int... params) {

        Assert.notNull(params, "params must not be null.");

        String[] s = new String[params.length];
        for (int i = 0; i < params.length; i++) {
            int param = params[i];
            s[i] = String.valueOf(param);
        }

        return isCorrect(s);
    }

    /**
     * 指定の<code>params</code>が日時の情報として適切な場合<code>true</code>を返します。
     * 例えば、2011年2月30日などは<code>false</code>となります。
     *
     * <p><code>params</code>にはフォーマッタに応じて年月日などの順に格納されている必要があります。
     *
     * @param params 日時の情報
     * @return 日時として適切な場合、<code>true</code>
     */
    public boolean isCorrect(String... params) {

        Assert.notNull(params, "params must not be null.");
        Assert.notEmpty(params, "params must not be empty.");

        for (String s : params) {
            if (StringUtils.isEmpty(s)) {
                return false;
            }
        }
        
        try {
            DateUtils.parseDateStrictly(toDateString(params), new String[]{getFormat()});

            return true;

        } catch (ParseException pe) {
            return false;
        }
    }

    /**
     * 指定の<code>params</code>で<code>Date</code>オブジェクトを生成します。
     * <p><code>params</code>にはフォーマッタに応じて年月日などの順に格納されている必要があります。
     *
     * @param params 日時の情報
     * @return <code>Date</code>オブジェクト
     * @throws IllegalArgumentException 日時の情報として適切でない場合にスロー
     */
    public Date newDate(String... params) {

        Assert.notNull(params, "params must not be null.");
        Assert.notEmpty(params, "params must not be empty.");

        int[] in = new int[params.length];

        for (int i=0; i<params.length; i++) {

            if (!NumberUtils.isDigits(params[i])) {
                throw new IllegalArgumentException(params[i] + " is not a digit.");
            }

            in[i] = Integer.parseInt(params[i]);
        }

        return newDate(in);
    }

    /**
     * 指定の<code>params</code>で<code>Date</code>オブジェクトを生成します。
     * <p><code>params</code>にはフォーマッタに応じて年月日などの順に格納されている必要があります。
     *
     * @param params 日時の情報
     * @return <code>Date</code>オブジェクト
     * @throws IllegalArgumentException 日時の情報として適切でない場合にスロー
     */
    public abstract Date newDate(int... params);

    /**
     * 指定の<code>params</code>の文字列形式にフォーマットした情報を返します。
     *
     * @param params 日時の情報
     * @return 文字列形式にフォーマットした情報
     */
    abstract String toDateString(String... params);



    /** DATE型の初期値：0001/01/01 */
    public static final String INITIAL_DATE_STRING = "0001/01/01";

    /** DATE型の初期値：0001/01/01 */
    private static final Date INITIAL_DATE;
    static {
        Date date = new Date();
        date = DateUtils.setYears(date, 1);
        date = DateUtils.setMonths(date, 0);
        date = DateUtils.setDays(date, 1);
        date = DateUtils.setHours(date, 0);
        date = DateUtils.setMinutes(date, 0);
        date = DateUtils.setSeconds(date, 0);

        INITIAL_DATE = date;
    }

    /** DATE型の最大値:9999/12/31 23:59:59 */
    public static final long MAX_DATE_LONG;
    static {
        Date date = new Date(INITIAL_DATE.getTime());
        date = DateUtils.setYears(date, 9999);
        date = DateUtils.setMonths(date, 11);
        date = DateUtils.setDays(date, 31);
        date = DateUtils.setHours(date, 23);
        date = DateUtils.setMinutes(date, 59);
        date = DateUtils.setSeconds(date, 59);

        MAX_DATE_LONG = date.getTime();
    }

    /**
     * <code>fromDate</code>と<code>toDate</code>を比較し
     * <code>fromDate</code>→<code>toDate</code>の順になっている場合<code>true</code>を返します。
     *
     * @param fromDate from日時
     * @param toDate to日時
     * @return <code>fromDate</code>→<code>toDate</code>の順になっている場合<code>true</code>
     */
    public static boolean isFromTo(Date fromDate, Date toDate) {

        Assert.notNull(fromDate, "fromDate must not be null.");
        Assert.notNull(toDate, "toDate must not be null.");

        return fromDate.compareTo(toDate) <= 0;
    }

    /**
     * 初期値を表す<code>Date</code>オブジェクトを返します。
     *
     * @return 初期値を表す<code>Date</code>オブジェクト
     */
    public static Date getInitialDate() {
        return INITIAL_DATE;
    }

    /**
     * ログクラスを返します。
     *
     * @return ログクラス
     */
    protected Log getLog() {
        return LogFactory.getLog(this.getClass().getName());
    }
}
