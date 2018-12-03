package com.neusoft.DouBanMovies;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class MovieInfoBean implements WritableComparable<MovieInfoBean> {

    private String MovieName;
    private float StarFive;

    public MovieInfoBean() {
    }

    public String getMovieName() {
        return MovieName;
    }

    public void setMovieName(String movieName) {
        MovieName = movieName;
    }

    public float getStarFive() {
        return StarFive;
    }

    public void setStarFive(float starFive) {
        StarFive = starFive;
    }

    @Override
    public int compareTo(MovieInfoBean o) {
        return this.StarFive - o.getStarFive() > 0 ? -1 : 1;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(this.MovieName);
        out.writeFloat(this.StarFive);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.MovieName = in.readUTF();
        this.StarFive = in.readFloat();
    }
}
