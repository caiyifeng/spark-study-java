package cn.spark.study;

import java.io.Serializable;

public class HospitalInfo implements Serializable {
	String hospitalName;
	String hospitalType;
	String hospitalClass;
	String hospitalLevel;
	String hospitalCapital;
	String companySpecial;
	String insuranceSpecial;
	String Province;
	String city;
	String address;
	String contact;
	String contactInfo;
	public String getHospitalName() {
		return hospitalName;
	}
	public void setHospitalName(String hospitalName) {
		this.hospitalName = hospitalName;
	}
	public String getHospitalType() {
		return hospitalType;
	}
	public void setHospitalType(String hospitalType) {
		this.hospitalType = hospitalType;
	}
	public String getHospitalClass() {
		return hospitalClass;
	}
	public void setHospitalClass(String hospitalClass) {
		this.hospitalClass = hospitalClass;
	}
	public String getHospitalLevel() {
		return hospitalLevel;
	}
	public void setHospitalLevel(String hospitalLevel) {
		this.hospitalLevel = hospitalLevel;
	}
	public String getHospitalCapital() {
		return hospitalCapital;
	}
	public void setHospitalCapital(String hospitalCapital) {
		this.hospitalCapital = hospitalCapital;
	}
	public String getCompanySpecial() {
		return companySpecial;
	}
	public void setCompanySpecial(String companySpecial) {
		this.companySpecial = companySpecial;
	}
	public String getInsuranceSpecial() {
		return insuranceSpecial;
	}
	public void setInsuranceSpecial(String insuranceSpecial) {
		this.insuranceSpecial = insuranceSpecial;
	}
	public String getProvince() {
		return Province;
	}
	public void setProvince(String province) {
		Province = province;
	}
	public String getCity() {
		return city;
	}
	public void setCity(String city) {
		this.city = city;
	}
	public String getAddress() {
		return address;
	}
	public void setAddress(String address) {
		this.address = address;
	}
	public String getContact() {
		return contact;
	}
	public void setContact(String contact) {
		this.contact = contact;
	}
	public String getContactInfo() {
		return contactInfo;
	}
	public void setContactInfo(String contactInfo) {
		this.contactInfo = contactInfo;
	}
	
	public void InitInfo(String[] infos) {
		if (infos.length == 12) {
			setHospitalName(infos[0]);
		}
	}
	
	public void mergeInfo(HospitalInfo info) {
		if (this.getHospitalName() == null || this.getHospitalName().length()==0) {
			if (info.getHospitalName() !=null && info.getHospitalName().length() >0)
				this.setHospitalName(info.getHospitalName());
		}
		if (this.getAddress() == null || this.getAddress().length()==0) {
			if (info.getAddress() !=null && info.getAddress().length() >0)
				this.setAddress(info.getAddress());
		}		
		if (this.getCity() == null || this.getCity().length()==0) {
			if (info.getCity() !=null && info.getCity().length() >0)
				this.setCity(info.getCity());
		}
		if (this.getCompanySpecial() == null || this.getCompanySpecial().length()==0) {
			if (info.getCompanySpecial() !=null && info.getCompanySpecial().length() >0)
				this.setCompanySpecial(info.getCompanySpecial());
		}	
		if (this.getContact() == null || this.getContact().length()==0) {
			if (info.getContact() !=null && info.getContact().length() >0)
				this.setContact(info.getContact());
		}	
		if (this.getContactInfo() == null || this.getContactInfo().length()==0) {
			if (info.getContactInfo() !=null && info.getContactInfo().length() >0)
				this.setContactInfo(info.getContactInfo());
		}	
		if (this.getHospitalCapital() == null || this.getHospitalCapital().length()==0) {
			if (info.getHospitalCapital() !=null && info.getHospitalCapital().length() >0)
				this.setHospitalCapital(info.getHospitalCapital());
		}	
		if (this.getHospitalClass() == null || this.getHospitalClass().length()==0) {
			if (info.getHospitalClass() !=null && info.getHospitalClass().length() >0)
				this.setHospitalClass(info.getHospitalClass());
		}
		if (this.getHospitalLevel() == null || this.getHospitalLevel().length()==0) {
			if (info.getHospitalLevel() !=null && info.getHospitalLevel().length() >0)
				this.setHospitalLevel(info.getHospitalLevel());
		}
		if (this.getHospitalType() == null || this.getHospitalType().length()==0) {
			if (info.getHospitalType() !=null && info.getHospitalType().length() >0)
				this.setHospitalType(info.getHospitalType());
		}
		if (this.getInsuranceSpecial() == null || this.getInsuranceSpecial().length()==0) {
			if (info.getInsuranceSpecial() !=null && info.getInsuranceSpecial().length() >0)
				this.setInsuranceSpecial(info.getInsuranceSpecial());
		}	
		
		if (this.getProvince() == null || this.getProvince().length()==0) {
			if (info.getProvince() !=null && info.getProvince().length() >0)
				this.setProvince(info.getProvince());
		}		
	}
	@Override
	public String toString() {
		return "HospitalInfo [hospitalName=" + hospitalName + ", hospitalType="
				+ hospitalType + ", hospitalClass=" + hospitalClass
				+ ", hospitalLevel=" + hospitalLevel + ", hospitalCapital="
				+ hospitalCapital + ", companySpecial=" + companySpecial
				+ ", insuranceSpecial=" + insuranceSpecial + ", Province="
				+ Province + ", city=" + city + ", address=" + address
				+ ", contact=" + contact + ", contactInfo=" + contactInfo + "]";
	}
	
}
