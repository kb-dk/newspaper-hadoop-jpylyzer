package dk.statsbiblioteket.medieplatform.hadoop;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;

public class JpylyzerMapperTest {


    @Before
    public void setUp() {
        //JpylyzerMapper mapper = new JpylyzerMapper("src/test/extras/jpylyzer-1.10.1/jpylyzer.py");
    }

    @Test
    public void testSimplest() throws IOException {
        MapDriver<LongWritable, Text, Text, Text> mapDriver;
        JpylyzerMapper mapper = new JpylyzerMapper();
        mapDriver = MapDriver.newMapDriver(mapper);

        mapDriver.withInput(new LongWritable(1), new Text("ein"));
        mapDriver.withOutput(new Text("ein"), new Text("ein"));
        mapDriver.runTest();
    }

    @Test
    public void testMapper() throws IOException, URISyntaxException {
//        MapDriver<LongWritable, Text, Text, Text> mapDriver;
//        JpylyzerMapper mapper = new JpylyzerMapper("src/test/extras/jpylyzer-1.10.1/jpylyzer.py");
//        mapDriver = MapDriver.newMapDriver(mapper);
//
//        String name = "test.jp2";
//        mapDriver.withInput(new LongWritable(1), new Text(getAbsolutePath(name)));
//        mapDriver.withOutput(new Text(getAbsolutePath(name)), new Text("<?xml version='1.0' encoding='UTF-8'?><jpylyzer><toolInfo><toolName>jpylyzer.py</toolName><toolVersion>1.10.1</toolVersion></toolInfo><fileInfo><fileName>test.jp2</fileName><filePath>/home/abr/Projects/newspaper-hadoop-jpylyzer/target/test-classes/test.jp2</filePath><fileSizeInBytes>3662332</fileSizeInBytes><fileLastModified>Tue Nov 26 11:31:58 2013</fileLastModified></fileInfo><isValidJP2>True</isValidJP2><tests /><properties><signatureBox /><fileTypeBox><br>jp2 </br><minV>0</minV><cL>jp2 </cL></fileTypeBox><jp2HeaderBox><imageHeaderBox><height>2859</height><width>2312</width><nC>1</nC><bPCSign>unsigned</bPCSign><bPCDepth>8</bPCDepth><c>jpeg2000</c><unkC>yes</unkC><iPR>no</iPR></imageHeaderBox><colourSpecificationBox><meth>Enumerated</meth><prec>0</prec><approx>0</approx><enumCS>greyscale</enumCS></colourSpecificationBox><resolutionBox><captureResolutionBox><vRcN>30000</vRcN><vRcD>254</vRcD><hRcN>30000</hRcN><hRcD>254</hRcD><vRcE>2</vRcE><hRcE>2</hRcE><vRescInPixelsPerMeter>11811.02</vRescInPixelsPerMeter><hRescInPixelsPerMeter>11811.02</hRescInPixelsPerMeter><vRescInPixelsPerInch>300.0</vRescInPixelsPerInch><hRescInPixelsPerInch>300.0</hRescInPixelsPerInch></captureResolutionBox></resolutionBox></jp2HeaderBox><contiguousCodestreamBox><siz><lsiz>41</lsiz><rsiz>ISO/IEC 15444-1</rsiz><xsiz>2312</xsiz><ysiz>2859</ysiz><xOsiz>0</xOsiz><yOsiz>0</yOsiz><xTsiz>1024</xTsiz><yTsiz>1024</yTsiz><xTOsiz>0</xTOsiz><yTOsiz>0</yTOsiz><numberOfTiles>9</numberOfTiles><csiz>1</csiz><ssizSign>unsigned</ssizSign><ssizDepth>8</ssizDepth><xRsiz>1</xRsiz><yRsiz>1</yRsiz></siz><cod><lcod>12</lcod><precincts>no</precincts><sop>yes</sop><eph>yes</eph><order>RPCL</order><layers>16</layers><multipleComponentTransformation>no</multipleComponentTransformation><levels>6</levels><codeBlockWidth>64</codeBlockWidth><codeBlockHeight>64</codeBlockHeight><codingBypass>yes</codingBypass><resetOnBoundaries>no</resetOnBoundaries><termOnEachPass>no</termOnEachPass><vertCausalContext>no</vertCausalContext><predTermination>no</predTermination><segmentationSymbols>yes</segmentationSymbols><transformation>5-3 reversible</transformation></cod><qcd><lqcd>22</lqcd><qStyle>no quantization</qStyle><guardBits>1</guardBits><epsilon>9</epsilon><epsilon>10</epsilon><epsilon>10</epsilon><epsilon>11</epsilon><epsilon>10</epsilon><epsilon>10</epsilon></qcd><com><lcom>15</lcom><rcom>ISO/IEC 8859-15 (Latin)</rcom><comment>Kakadu-v7.2</comment></com><com><lcom>347</lcom><rcom>ISO/IEC 8859-15 (Latin)</rcom><comment>Kdu-Layer-Info: log_2{Delta-D(squared-error)/Delta-L(bytes)}, L(bytes)*-175.6, 3.6e+006*-176.8, 3.6e+006*-177.9, 3.6e+006*-179.1, 3.6e+006*-180.3, 3.6e+006*-181.5, 3.7e+006*-182.6, 3.7e+006*-183.8, 3.7e+006*-185.0, 3.7e+006*-186.1, 3.7e+006*-187.3, 3.7e+006*-188.5, 3.7e+006*-189.7, 3.7e+006*-190.8, 3.7e+006*-192.0, 3.7e+006*-192.0, 6.6e+006*</comment></com><tileParts><tilePart><sot><lsot>10</lsot><isot>0</isot><psot>415</psot><tpsot>0</tpsot><tnsot>0</tnsot></sot></tilePart><tilePart><sot><lsot>10</lsot><isot>1</isot><psot>406</psot><tpsot>0</tpsot><tnsot>0</tnsot></sot></tilePart><tilePart><sot><lsot>10</lsot><isot>2</isot><psot>246</psot><tpsot>0</tpsot><tnsot>0</tnsot></sot></tilePart><tilePart><sot><lsot>10</lsot><isot>3</isot><psot>394</psot><tpsot>0</tpsot><tnsot>0</tnsot></sot></tilePart><tilePart><sot><lsot>10</lsot><isot>4</isot><psot>403</psot><tpsot>0</tpsot><tnsot>0</tnsot></sot></tilePart><tilePart><sot><lsot>10</lsot><isot>5</isot><psot>238</psot><tpsot>0</tpsot><tnsot>0</tnsot></sot></tilePart><tilePart><sot><lsot>10</lsot><isot>6</isot><psot>353</psot><tpsot>0</tpsot><tnsot>0</tnsot></sot></tilePart><tilePart><sot><lsot>10</lsot><isot>7</isot><psot>346</psot><tpsot>0</tpsot><tnsot>0</tnsot></sot></tilePart><tilePart><sot><lsot>10</lsot><isot>8</isot><psot>229</psot><tpsot>0</tpsot><tnsot>0</tnsot></sot></tilePart><tilePart><sot><lsot>10</lsot><isot>0</isot><psot>923</psot><tpsot>1</tpsot><tnsot>0</tnsot></sot></tilePart><tilePart><sot><lsot>10</lsot><isot>1</isot><psot>923</psot><tpsot>1</tpsot><tnsot>0</tnsot></sot></tilePart><tilePart><sot><lsot>10</lsot><isot>2</isot><psot>368</psot><tpsot>1</tpsot><tnsot>0</tnsot></sot></tilePart><tilePart><sot><lsot>10</lsot><isot>3</isot><psot>942</psot><tpsot>1</tpsot><tnsot>0</tnsot></sot></tilePart><tilePart><sot><lsot>10</lsot><isot>4</isot><psot>941</psot><tpsot>1</tpsot><tnsot>0</tnsot></sot></tilePart><tilePart><sot><lsot>10</lsot><isot>5</isot><psot>345</psot><tpsot>1</tpsot><tnsot>0</tnsot></sot></tilePart><tilePart><sot><lsot>10</lsot><isot>6</isot><psot>655</psot><tpsot>1</tpsot><tnsot>0</tnsot></sot></tilePart><tilePart><sot><lsot>10</lsot><isot>7</isot><psot>659</psot><tpsot>1</tpsot><tnsot>0</tnsot></sot></tilePart><tilePart><sot><lsot>10</lsot><isot>8</isot><psot>326</psot><tpsot>1</tpsot><tnsot>0</tnsot></sot></tilePart><tilePart><sot><lsot>10</lsot><isot>0</isot><psot>2999</psot><tpsot>2</tpsot><tnsot>0</tnsot></sot></tilePart><tilePart><sot><lsot>10</lsot><isot>1</isot><psot>2935</psot><tpsot>2</tpsot><tnsot>0</tnsot></sot></tilePart><tilePart><sot><lsot>10</lsot><isot>2</isot><psot>818</psot><tpsot>2</tpsot><tnsot>0</tnsot></sot></tilePart><tilePart><sot><lsot>10</lsot><isot>3</isot><psot>3228</psot><tpsot>2</tpsot><tnsot>0</tnsot></sot></tilePart><tilePart><sot><lsot>10</lsot><isot>4</isot><psot>3258</psot><tpsot>2</tpsot><tnsot>0</tnsot></sot></tilePart><tilePart><sot><lsot>10</lsot><isot>5</isot><psot>744</psot><tpsot>2</tpsot><tnsot>0</tnsot></sot></tilePart><tilePart><sot><lsot>10</lsot><isot>6</isot><psot>1866</psot><tpsot>2</tpsot><tnsot>0</tnsot></sot></tilePart><tilePart><sot><lsot>10</lsot><isot>7</isot><psot>1892</psot><tpsot>2</tpsot><tnsot>0</tnsot></sot></tilePart><tilePart><sot><lsot>10</lsot><isot>8</isot><psot>638</psot><tpsot>2</tpsot><tnsot>0</tnsot></sot></tilePart><tilePart><sot><lsot>10</lsot><isot>0</isot><psot>10644</psot><tpsot>3</tpsot><tnsot>0</tnsot></sot></tilePart><tilePart><sot><lsot>10</lsot><isot>1</isot><psot>10571</psot><tpsot>3</tpsot><tnsot>0</tnsot></sot></tilePart><tilePart><sot><lsot>10</lsot><isot>2</isot><psot>2420</psot><tpsot>3</tpsot><tnsot>0</tnsot></sot></tilePart><tilePart><sot><lsot>10</lsot><isot>3</isot><psot>12259</psot><tpsot>3</tpsot><tnsot>0</tnsot></sot></tilePart><tilePart><sot><lsot>10</lsot><isot>4</isot><psot>12242</psot><tpsot>3</tpsot><tnsot>0</tnsot></sot></tilePart><tilePart><sot><lsot>10</lsot><isot>5</isot><psot>2215</psot><tpsot>3</tpsot><tnsot>0</tnsot></sot></tilePart><tilePart><sot><lsot>10</lsot><isot>6</isot><psot>6506</psot><tpsot>3</tpsot><tnsot>0</tnsot></sot></tilePart><tilePart><sot><lsot>10</lsot><isot>7</isot><psot>6737</psot><tpsot>3</tpsot><tnsot>0</tnsot></sot></tilePart><tilePart><sot><lsot>10</lsot><isot>8</isot><psot>1757</psot><tpsot>3</tpsot><tnsot>0</tnsot></sot></tilePart><tilePart><sot><lsot>10</lsot><isot>0</isot><psot>38750</psot><tpsot>4</tpsot><tnsot>0</tnsot></sot></tilePart><tilePart><sot><lsot>10</lsot><isot>1</isot><psot>38006</psot><tpsot>4</tpsot><tnsot>0</tnsot></sot></tilePart><tilePart><sot><lsot>10</lsot><isot>2</isot><psot>8271</psot><tpsot>4</tpsot><tnsot>0</tnsot></sot></tilePart><tilePart><sot><lsot>10</lsot><isot>3</isot><psot>46375</psot><tpsot>4</tpsot><tnsot>0</tnsot></sot></tilePart><tilePart><sot><lsot>10</lsot><isot>4</isot><psot>45894</psot><tpsot>4</tpsot><tnsot>0</tnsot></sot></tilePart><tilePart><sot><lsot>10</lsot><isot>5</isot><psot>7668</psot><tpsot>4</tpsot><tnsot>0</tnsot></sot></tilePart><tilePart><sot><lsot>10</lsot><isot>6</isot><psot>23644</psot><tpsot>4</tpsot><tnsot>0</tnsot></sot></tilePart><tilePart><sot><lsot>10</lsot><isot>7</isot><psot>25645</psot><tpsot>4</tpsot><tnsot>0</tnsot></sot></tilePart><tilePart><sot><lsot>10</lsot><isot>8</isot><psot>5646</psot><tpsot>4</tpsot><tnsot>0</tnsot></sot></tilePart><tilePart><sot><lsot>10</lsot><isot>0</isot><psot>131213</psot><tpsot>5</tpsot><tnsot>0</tnsot></sot></tilePart><tilePart><sot><lsot>10</lsot><isot>1</isot><psot>130633</psot><tpsot>5</tpsot><tnsot>0</tnsot></sot></tilePart><tilePart><sot><lsot>10</lsot><isot>2</isot><psot>28136</psot><tpsot>5</tpsot><tnsot>0</tnsot></sot></tilePart><tilePart><sot><lsot>10</lsot><isot>3</isot><psot>156049</psot><tpsot>5</tpsot><tnsot>0</tnsot></sot></tilePart><tilePart><sot><lsot>10</lsot><isot>4</isot><psot>154013</psot><tpsot>5</tpsot><tnsot>0</tnsot></sot></tilePart><tilePart><sot><lsot>10</lsot><isot>5</isot><psot>26877</psot><tpsot>5</tpsot><tnsot>0</tnsot></sot></tilePart><tilePart><sot><lsot>10</lsot><isot>6</isot><psot>84513</psot><tpsot>5</tpsot><tnsot>0</tnsot></sot></tilePart><tilePart><sot><lsot>10</lsot><isot>7</isot><psot>92448</psot><tpsot>5</tpsot><tnsot>0</tnsot></sot></tilePart><tilePart><sot><lsot>10</lsot><isot>8</isot><psot>19126</psot><tpsot>5</tpsot><tnsot>0</tnsot></sot></tilePart><tilePart><sot><lsot>10</lsot><isot>0</isot><psot>398182</psot><tpsot>6</tpsot><tnsot>7</tnsot></sot></tilePart><tilePart><sot><lsot>10</lsot><isot>1</isot><psot>397002</psot><tpsot>6</tpsot><tnsot>7</tnsot></sot></tilePart><tilePart><sot><lsot>10</lsot><isot>2</isot><psot>85024</psot><tpsot>6</tpsot><tnsot>7</tnsot></sot></tilePart><tilePart><sot><lsot>10</lsot><isot>3</isot><psot>470040</psot><tpsot>6</tpsot><tnsot>7</tnsot></sot></tilePart><tilePart><sot><lsot>10</lsot><isot>4</isot><psot>460636</psot><tpsot>6</tpsot><tnsot>7</tnsot></sot></tilePart><tilePart><sot><lsot>10</lsot><isot>5</isot><psot>83630</psot><tpsot>6</tpsot><tnsot>7</tnsot></sot></tilePart><tilePart><sot><lsot>10</lsot><isot>6</isot><psot>264834</psot><tpsot>6</tpsot><tnsot>7</tnsot></sot></tilePart><tilePart><sot><lsot>10</lsot><isot>7</isot><psot>289525</psot><tpsot>6</tpsot><tnsot>7</tnsot></sot></tilePart><tilePart><sot><lsot>10</lsot><isot>8</isot><psot>57149</psot><tpsot>6</tpsot><tnsot>7</tnsot></sot></tilePart></tileParts></contiguousCodestreamBox><compressionRatio>1.8</compressionRatio></properties></jpylyzer>"));
//        mapDriver.runTest();
    }

    private String getAbsolutePath(String name) throws URISyntaxException {
        return new File(Thread.currentThread().getContextClassLoader().getResource(
                name).toURI()).getAbsolutePath();
    }

}
