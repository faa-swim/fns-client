package us.dot.faa.swim.fns;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.zip.GZIPInputStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.jcraft.jsch.Channel;
import com.jcraft.jsch.ChannelSftp;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.Session;
import com.jcraft.jsch.SftpException;

public class FilClient {
	private final static Logger logger = LoggerFactory.getLogger(FilClient.class);

	final JSch jsch = new JSch();
	Session jschSession = null;
	ChannelSftp sftpChannel = null;
	
	private String remoteHost = "";
	private int remotePort = 22;
	private String username = "";
	private String strictHostKeyChecking = "no";
	private String knownHostsFilePath = "";
	private String certFilePath = "";
	private String filDateFileName = "primary_last_date.txt";
	private String filDataFileTimeFormat = "yyyy-MM-dd HH:mm:ss z";
	private String filFileName = "initial_load_aixm.xml.gz";
	private String filFileSavePath = null;

	public FilClient(final String remoteHost, final int remotePort, final String username,
			final String strictHostKeyChecking, final String knownHostsFilePath, final String certFilePath) throws JSchException {
		this.remoteHost = remoteHost;
		this.remotePort = remotePort;
		this.username = username;
		this.strictHostKeyChecking = strictHostKeyChecking;
		this.knownHostsFilePath = knownHostsFilePath;
		this.certFilePath = certFilePath;
		
		initializeFilClient();
	}

	public FilClient(final String remoteHost, final String username, final String certFilePath) throws JSchException {
		this.remoteHost = remoteHost;
		this.username = username;
		this.certFilePath = certFilePath;
		
		initializeFilClient();
	}
	
	private void initializeFilClient() throws JSchException
	{
		if (!knownHostsFilePath.isEmpty()) {
			final String knownHostsFilename = knownHostsFilePath;
			this.jsch.setKnownHosts(knownHostsFilename);
		}

		jsch.addIdentity(certFilePath);
	}

	public void setFilFileSavePath(String filFileSavePath)
	{
		this.filFileSavePath = filFileSavePath;
	}
	
	public void connectToFil() throws JSchException
	{
		logger.info("Connecting to FNS Initial Load SFTP server");
		
		jschSession = jsch.getSession(username, remoteHost, remotePort);

		java.util.Properties config = new java.util.Properties();
		config.put("StrictHostKeyChecking", strictHostKeyChecking);
		jschSession.setConfig(config);
		jschSession.connect();

		final Channel channel = jschSession.openChannel("sftp");
		channel.connect();

		sftpChannel = (ChannelSftp) channel;
	}
	
	public void close()
	{
		if (sftpChannel != null) {
			sftpChannel.exit();
		}

		if (jschSession != null) {
			jschSession.disconnect();
		}
	}
	
	public InputStream getFnsInitialLoad() throws SftpException, ParseException, InterruptedException, IOException  {
		logger.info("Getting most recent FNS Initial Load File from SFTP server");
		
		InputStream inputStream_primary_last_date = null;
		BufferedReader bufferedReader = null;

		try {
			

			// check fil last update and wait for next update then get file
			final Date refDate = new Date(System.currentTimeMillis());
			boolean mostRecentFilFileAvailable = false;
			SimpleDateFormat formatter = new SimpleDateFormat(filDataFileTimeFormat);
			String primary_last_date = "";
			Date modTime = null;
			String filFileLocalPath = "";
			
			while (!mostRecentFilFileAvailable) {
				
				try
				{				
					inputStream_primary_last_date = sftpChannel.get(filDateFileName);
				}
				catch (SftpException e)
				{
					logger.info("FIL Date Time File Not Available, waiting...");
					Thread.sleep(1000 * 5 * 1);
					continue;
				}
				
				bufferedReader = new BufferedReader(new InputStreamReader(inputStream_primary_last_date));
				primary_last_date = bufferedReader.lines().reduce("", String::concat).trim() + " UTC";
				
				modTime = formatter.parse(primary_last_date);

				if (modTime.compareTo(refDate) >= 0) {
					mostRecentFilFileAvailable = true;
					logger.info("New FNS Initial Load File found with modified time: " + modTime.toString());
				} else {
					// wait for one minute then check for new fil file again
					logger.info(
							"Waiting for New FNS Initial Load File, current file modified time: " + modTime.toString());
					Thread.sleep(1000 * 60 * 1);
				}
			}

			if (filFileSavePath != null) {
				filFileLocalPath = filFileSavePath.concat(filFileName);
				sftpChannel.get(filFileName, filFileLocalPath);
				return new GZIPInputStream(new FileInputStream(filFileLocalPath));
			} else {
				return new GZIPInputStream(sftpChannel.get(filFileName));
			}
			
			
			//need to update to that the stream stays open and then call close on the client later
			

		} catch (SftpException | ParseException | InterruptedException | IOException e) {
			throw e;
		} finally {			

			if (inputStream_primary_last_date != null) {
				try {
					inputStream_primary_last_date.close();
				} catch (IOException e) {
					logger.error(e.getMessage());
				}
			}

			if (bufferedReader != null) {
				try {
					bufferedReader.close();
				} catch (IOException e) {
					logger.error(e.getMessage());
				}
			}
		}
	}

}
