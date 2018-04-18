package cashflow;

import java.io.Serializable;

public class Aggs implements Serializable {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	public Float sum;
	public Integer count;

	public Aggs(Integer count, Float sum) {
		this.sum = sum;
		this.count = count;
	}

	public Aggs add(Aggs v) {
		return new Aggs(count + v.count, sum + v.sum);
	}

	public String toString() {
		return String.format("{\"count\":%d, \"sum\":%f}", count, sum);
	}

}
