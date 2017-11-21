package com.wyu.transformer.model.dim.base;

import com.wyu.commom.DateEnum;
import com.wyu.util.TimeUtil;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Calendar;
import java.util.Date;

/**
 * 时间维度信息类
 *
 * @author ken
 */
public class DateDimension extends BaseDimension {

    private int id;
    private int year;
    private int season;
    private int month;
    private int week;
    private int day;
    private String type;
    private Date calendar = new Date();

    public DateDimension() {
    }

    public DateDimension(int year, int season, int month, int week, int day, String type) {
        this.year = year;
        this.season = season;
        this.month = month;
        this.week = week;
        this.day = day;
        this.type = type;
    }

    public DateDimension(int year, int season, int month, int week, int day, String type, Date calendar) {
        this(year, season, month, week, day, type);
        this.calendar = calendar;
    }

    public DateDimension(int id, int year, int season, int month, int week, int day, String type, Date calendar) {
        this(year, season, month, week, day, type, calendar);
        this.id = id;

    }

    /**
     * 根据type类型获取时间维度对象
     *
     * @param time 时间戳
     * @param type 类型
     * @return
     */
    public static DateDimension buildDate(long time, DateEnum type) {

        int year = TimeUtil.getDateInfo(time, DateEnum.YEAR);
        Calendar calendar = Calendar.getInstance();
        calendar.clear();

        if (DateEnum.YEAR.equals(type)) {
            calendar.set(year, 0, 1);
            return new DateDimension(year, 0, 0, 0, 0, type.name, calendar.getTime());
        }
        int season = TimeUtil.getDateInfo(time, DateEnum.SEASON);
        if (DateEnum.SEASON.equals(type)) {
            int month = (3 * season - 2);
            calendar.set(year, month - 1, 1);
            return new DateDimension(year, season, 0, 0, 0, type.name, calendar.getTime());
        }
        int month = TimeUtil.getDateInfo(time, DateEnum.MONTH);
        if (DateEnum.MONTH.equals(type)) {
            calendar.set(year, month - 1, 1);
            return new DateDimension(year, season, month, 0, 0, type.name, calendar.getTime());

        }

        int week = TimeUtil.getDateInfo(time, DateEnum.WEEK);
        if (DateEnum.WEEK.equals(type)) {
            long firstDayOfWeek = TimeUtil.getFirstDayOfThisWeek(time);
            year = TimeUtil.getDateInfo(firstDayOfWeek, DateEnum.YEAR);
            season = TimeUtil.getDateInfo(firstDayOfWeek, DateEnum.SEASON);
            month = TimeUtil.getDateInfo(firstDayOfWeek, DateEnum.MONTH);
            week = TimeUtil.getDateInfo(firstDayOfWeek, DateEnum.WEEK);
            if (month == 12 && week == 1) {
                week = 53;
            }
            return new DateDimension(year, season, month, week, 0, type.name, new Date(firstDayOfWeek));
        }
        int day = TimeUtil.getDateInfo(time, DateEnum.DAY);
        if (DateEnum.DAY.equals(type)) {
            calendar.set(year, month - 1, day);
            if (month == 12 && week == 1) {
                week = 53;
            }
            return new DateDimension(year, season, month, week, day, type.name, calendar.getTime());

        }
        throw new RuntimeException("不支持DateEnum类型类获取DateDimension对象：" + type);
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public int getYear() {
        return year;
    }

    public void setYear(int year) {
        this.year = year;
    }

    public int getSeason() {
        return season;
    }

    public void setSeason(int season) {
        this.season = season;
    }

    public int getMonth() {
        return month;
    }

    public void setMonth(int month) {
        this.month = month;
    }

    public int getWeek() {
        return week;
    }

    public void setWeek(int week) {
        this.week = week;
    }

    public int getDay() {
        return day;
    }

    public void setDay(int day) {
        this.day = day;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public Date getCalendar() {
        return calendar;
    }

    public void setCalendar(Date calendar) {
        this.calendar = calendar;
    }

    @Override
    public int compareTo(BaseDimension o) {
        if (this == o) {
            return 0;
        }
        DateDimension other = (DateDimension) o;
        int tmp = Integer.compare(this.id, other.id);
        if (tmp != 0) {
            return tmp;
        }
        tmp = Integer.compare(this.id, other.id);
        if (tmp != 0) {
            return tmp;
        }
        tmp = Integer.compare(this.year, other.year);
        if (tmp != 0) {
            return tmp;
        }
        tmp = Integer.compare(this.month, other.month);
        if (tmp != 0) {
            return tmp;
        }
        tmp = Integer.compare(this.week, other.week);
        if (tmp != 0) {
            return tmp;
        }
        tmp = Integer.compare(this.day, other.day);
        if (tmp != 0) {
            return tmp;
        }
        tmp = this.type.compareTo(other.type);
        return tmp;


    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(this.id);
        out.writeInt(this.year);
        out.writeInt(this.season);
        out.writeInt(this.month);
        out.writeInt(this.week);
        out.writeInt(this.day);
        out.writeUTF(this.type);
        out.writeLong(this.calendar.getTime());

    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.id = in.readInt();
        this.year = in.readInt();
        this.season = in.readInt();
        this.month = in.readInt();
        this.week = in.readInt();
        this.day = in.readInt();
        this.type = in.readUTF();
        this.calendar.setTime(in.readLong());
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        DateDimension that = (DateDimension) o;

        if (id != that.id) {
            return false;
        }
        if (year != that.year) {
            return false;
        }
        if (season != that.season) {
            return false;
        }
        if (month != that.month) {
            return false;
        }
        if (week != that.week) {
            return false;
        }
        if (day != that.day) {
            return false;
        }
        return type != null ? type.equals(that.type) : that.type == null;
    }

    @Override
    public int hashCode() {
        int result = id;
        result = 31 * result + year;
        result = 31 * result + season;
        result = 31 * result + month;
        result = 31 * result + week;
        result = 31 * result + day;
        result = 31 * result + (type != null ? type.hashCode() : 0);
        return result;
    }
}
