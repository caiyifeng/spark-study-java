package cn.spark.study;

import java.io.Serializable;

public class MedicinalInfo implements  Serializable{
	String icdCode;
	String icdName;
	String icdRisk;
	String icdAccident;
	String icdDisease;
	public String getIcdCode() {
		return icdCode;
	}
	public void setIcdCode(String icdCode) {
		this.icdCode = icdCode;
	}
	public String getIcdName() {
		return icdName;
	}
	public void setIcdName(String icdName) {
		this.icdName = icdName;
	}
	public String getIcdRisk() {
		return icdRisk;
	}
	public void setIcdRisk(String icdRisk) {
		this.icdRisk = icdRisk;
	}
	public String getIcdAccident() {
		return icdAccident;
	}
	public void setIcdAccident(String icdAccident) {
		this.icdAccident = icdAccident;
	}
	public String getIcdDisease() {
		return icdDisease;
	}
	public void setIcdDisease(String icdDisease) {
		this.icdDisease = icdDisease;
	}
	
	public void mergeInfo(MedicinalInfo info) {
		if (this.getIcdCode() == null || this.getIcdCode().length() == 0) {
			if (info.getIcdCode() !=null && info.getIcdCode().length() >0)
				this.setIcdCode(info.getIcdCode());
		}
		
		if (this.getIcdName() == null || this.getIcdName().length() == 0) {
			if (info.getIcdName() !=null && info.getIcdName().length() >0)
				this.setIcdName(info.getIcdName());
		}
			
		
		if (this.getIcdAccident() == null || this.getIcdAccident().length()==0) {
			if (info.getIcdAccident() !=null && info.getIcdAccident().length() >0)
				this.setIcdAccident(info.getIcdAccident());
		}
		
		
		if (this.getIcdDisease() == null || this.getIcdDisease().length()==0) {
			if (info.getIcdDisease() !=null && info.getIcdDisease().length() >0)
				this.setIcdDisease(info.getIcdDisease());
		}
		
		
		if (this.getIcdRisk() == null || this.getIcdRisk().length()==0) {
			if (info.getIcdRisk() !=null && info.getIcdRisk().length() >0)
				this.setIcdRisk(info.getIcdRisk());
		}	
		
	}
	
}
