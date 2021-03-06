package bigs.modules.fe.global;

import java.util.ArrayList;
import java.util.List;

import bigs.api.fe.FeatureExtractionTask;
import bigs.modules.fe.utils.*;


/**
 * GrayHistogram.java
 * DescriptionClass
 * bigs
 * @created		Created on 6 March of 2012
 * @author 		jccaicedo
 * @author		aacruzr
 * @version 	%I%, %G%
 * @since 		1.5
 * @history
 * 06/03/2012	GrayHistogram.java
 * @copyright 	Copyright 2007-2012 (c) BioIngenium Research Group - Universidad Nacional de Colombia
 */

public class GrayHistogram extends FeatureExtractionTask {
	
	@Override
	public String toString() {
		StringBuffer sb = new StringBuffer();
		sb.append(this.getClass().getSimpleName());
		return sb.toString();
	}	
	
	@Override
	public List<List<Double>> extractFeatures(byte[] source) {			
		Image image = new Image(source); 
		int[] values = image.getGrayImage().getHistogram();//.getColorImage().getHistogram();
		
		List<List<Double>> data = new ArrayList<List<Double>>(); 
		
		List<Double> descriptor = new ArrayList<Double>();

        for(int i=0 ; i<values.length ; i++)
            descriptor.add((double)values[i]);		
        data.add(descriptor);
		return data;
	}

}
