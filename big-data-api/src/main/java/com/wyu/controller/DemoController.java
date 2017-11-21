package com.wyu.controller;

import java.util.HashMap;
import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;

import com.wyu.service.UserService;

@Controller
public class DemoController {
	@Autowired
	private UserService userService;

	@RequestMapping(value = "/test/getOneUser", method = RequestMethod.GET)
	@ResponseBody
	public Object test1() {
		return this.userService.getOneUser().toString();
	}


	@RequestMapping(value = "/test/json", method = RequestMethod.GET)
	@ResponseBody
	public Object test3() {
		Map<String, Integer> map = new HashMap<String, Integer>(16);
		map.put("周一", 15);
		map.put("周二", 18);
		map.put("周三", 10);
		map.put("周四", 8);
		map.put("周五", 11);
		map.put("周六", 13);
		map.put("周天", 9);
		return map;
	}

	@RequestMapping("/index")
	public String index() {
		return "index";
	}
	
	@RequestMapping("login")
	public String login(){
		return "login";
	}

}
