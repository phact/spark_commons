/*
 * Copyright 2015 Foundational Development
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package pro.foundev.javaexamples;

import java.io.Serializable;
import java.math.BigDecimal;
import java.util.Date;

public class Transaction implements Serializable{
    private String name;
    private String merchant;
    private BigDecimal amount;
    private Date transactionDate;
    private final String tranId;

    public String getName() {
        return name;
    }

    public String getMerchant() {
        return merchant;
    }

    public BigDecimal getAmount() {
        return amount;
    }

    public Date getTransactionDate() {
        return transactionDate;
    }

    public String getTranId() {
        return tranId;
    }

    public Transaction(String name, String merchant, BigDecimal amount, Date transactionDate, String tranId) {

        this.name = name;
        this.merchant = merchant;
        this.amount = amount;
        this.transactionDate = transactionDate;
        this.tranId = tranId;
    }

}
