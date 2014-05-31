package com.jukuad.statistic.pojo;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;


public class DateDeserializer extends JsonDeserializer<Long>{
	
	@Override
	public Long deserialize(JsonParser jsonParser, DeserializationContext deserializationContext)
			throws IOException, JsonProcessingException 
	{
		String unformatedDate= jsonParser.getText();
		SimpleDateFormat sdf= new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
		Date retVal;
		try {
			retVal = sdf.parse(unformatedDate);
		} catch (ParseException e) {
			return null;
		}
		return retVal.getTime();
	}
	
}