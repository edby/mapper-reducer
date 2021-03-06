package com.mapper.sort;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class pageCount implements WritableComparable<pageCount> {

        private String page;
        private int count;

        public void set(String page, int count) {
            this.page = page;
            this.count = count;
        }

        public String getPage() {
            return page;
        }
        public void setPage(String page) {
            this.page = page;
        }
        public int getCount() {
            return count;
        }
        public void setCount(int count) {
            this.count = count;
        }

        @Override
        public int compareTo(pageCount o) {

            return o.getCount()-this.count==0?this.page.compareTo(o.getPage()):o.getCount()-this.count;
        }


        @Override
        public void write(DataOutput out) throws IOException {
            out.writeUTF(this.page);
            out.writeInt(this.count);
        }

        @Override
        public void readFields(DataInput in) throws IOException {
            this.page = in.readUTF();
            this.count= in.readInt();
        }

        @Override
        public String toString() {
            return this.page + ',' + count;
        }
}



