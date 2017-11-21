package com.wyu.model;

/**
 * 对应数据库的model类
 * 
 * @author gerry
 *
 */
public class User implements java.io.Serializable {

	private static final long serialVersionUID = 1101160720279300694L;
	private Integer id;
	private String name;
	private String email;

	public Integer getId() {
		return id;
	}

	public void setId(Integer id) {
		this.id = id;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public String getEmail() {
		return email;
	}

	public void setEmail(String email) {
		this.email = email;
	}

}
