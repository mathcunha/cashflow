package cashflow;

import java.io.IOException;
import java.io.Serializable;
import java.util.Date;

import com.fasterxml.jackson.databind.ObjectMapper;

public class Transaction implements Serializable{

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private Date date;
	private String costumer;
	private String account;
	private Float value;
	private String city;

	@Override
	public String toString() {
		return "Transaction [getDate()=" + getDate() + ", getCostumer()=" + getCostumer() + ", getAccount()="
				+ getAccount() + ", getValue()=" + getValue() + ", getCity()=" + getCity() + "]";
	}

	public Date getDate() {
		return date;
	}

	public void setDate(Date date) {
		this.date = date;
	}

	public String getCostumer() {
		return costumer;
	}

	public void setCostumer(String costumer) {
		this.costumer = costumer;
	}

	public String getAccount() {
		return account;
	}

	public void setAccount(String account) {
		this.account = account;
	}

	public Float getValue() {
		return value;
	}

	public void setValue(Float value) {
		this.value = value;
	}

	public String getCity() {
		return city;
	}

	public void setCity(String city) {
		this.city = city;
	}
	
	public static Transaction fromJson(String payload) {
		ObjectMapper objectMapper = new ObjectMapper();
		Transaction t = null;
		try {
			t = objectMapper.readValue(payload, Transaction.class);
		} catch (IOException e) {
			e.printStackTrace();
		}
		return t;
	}
}
