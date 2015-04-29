/*
 *  Copyright 2015 Foundational Development
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 *
 */

package pro.foundev.examples.spark_streaming.java.dto;

import java.math.BigDecimal;
import java.util.Date;

public class TransactionTotal {

    private BigDecimal amount = new BigDecimal(0);
    private long count = 0;
    private BigDecimal max = new BigDecimal(0);
    private Date lastTransaction = new Date(0);

    public BigDecimal getAmount() {
        return amount;
    }

    public void setAmount(BigDecimal amount) {
        this.amount = amount;
    }

    public long getCount() {
        return count;
    }

    public void setCount(long count) {
        this.count = count;
    }

    public BigDecimal getMax() {
        return max;
    }

    public void setMax(BigDecimal max) {
        this.max = max;
    }

    public BigDecimal getAvg() {
        if(count==0||amount.equals(new BigDecimal(0))){
            return new BigDecimal(0);
        }
        return amount.divide(new BigDecimal(count), 2, BigDecimal.ROUND_HALF_EVEN);
    }

    public Date getLastTransaction() {
        return lastTransaction;
    }

    public void setLastTransaction(Date lastTransaction) {
        this.lastTransaction = lastTransaction;
    }

    public TransactionTotal add(TransactionTotal total){
        TransactionTotal newTotal = new TransactionTotal();
        newTotal.setAmount(this.amount.add(total.getAmount()));
        newTotal.setCount(this.count + total.getCount());
        newTotal.setMax(this.max.max(total.getMax()));
        Date newDate;
        if(this.lastTransaction.after(total.lastTransaction)){
           newDate = lastTransaction;
        }else{
            newDate = total.lastTransaction;
        }
        newTotal.setLastTransaction(newDate);
        return newTotal;
    }
}
