package bigs.modules.fe.global;

import ij.ImagePlus;
import ij.gui.NewImage;
import ij.process.Blitter;
import ij.process.ImageProcessor;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import bigs.api.core.BIGSParam;
import bigs.api.fe.FeatureExtractionTask;
import bigs.api.utils.TextUtils;
import bigs.modules.fe.global.mpeg7.EdgeHistogram;
import bigs.modules.fe.utils.*;


/**
 * MPEG7EdgeHistogram.java
 * DescriptionClass
 * bigs
 * @created		Created on 10 March of 2012
 * @author		aacruzr
 * @version 	%I%, %G%
 * @since 		1.5
 * @history
 * 06/03/2012	MPEG7EdgeHistogram.java
 * @copyright 	Copyright 2007-2012 (c) BioIngenium Research Group - Universidad Nacional de Colombia
 */

public class MPEG7EdgeHistogram extends FeatureExtractionTask {
	
	@Override
	public String toString() {
		StringBuffer sb = new StringBuffer();
		sb.append(this.getClass().getSimpleName());
		return sb.toString();
	}	
	
	@Override
	public List<List<Double>> extractFeatures(byte[] source) {			
		Image image = new Image(source); 
		
		List<List<Double>> data = new ArrayList<List<Double>>();
		
		List<Double> descriptor = new ArrayList<Double>();
				
		ImageProcessor ip = image.getGrayImage();
        image.setGrayImage(ip);
        EdgeHistogram EH = new EdgeHistogram(image.getBufferedImage());
        int values[] = EH.getHistogram();
        
        for(int i=0 ; i<values.length ; i++)
            descriptor.add((double)values[i]);
		
        data.add(descriptor);
		return data;
	}

}
