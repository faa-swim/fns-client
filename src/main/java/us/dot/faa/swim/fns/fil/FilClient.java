package us.dot.faa.swim.fns.fil;

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
	private FilClientConfig config;

	public FilClient(FilClientConfig config) throws JSchException {		
		this.config = config;
		initializeFilClient();
	}
	
	private void initializeFilClient() throws JSchException
	{
		if (this.config.sftpKnownHostsFilePath != null && !this.config.sftpKnownHostsFilePath.isEmpty()) {
			final String knownHostsFilename = this.config.sftpKnownHostsFilePath;
			this.jsch.setKnownHosts(knownHostsFilename);
		}

		jsch.addIdentity(this.config.sftpCertFilePath);
	}
	
	public void connectToFil() throws JSchException
	{
		logger.info("Connecting to FNS Initial Load SFTP server");
		
		jschSession = jsch.getSession(this.config.sftpUsername, this.config.sftpHost, this.config.sftpPort);

		java.util.Properties jschConfig = new java.util.Properties();
		jschConfig.put("StrictHostKeyChecking", this.config.sftpStrictHostKeyChecking);
		jschSession.setConfig(jschConfig);
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
			SimpleDateFormat formatter = new SimpleDateFormat(this.config.filDataFileTimeFormat);
			String primary_last_date = "";
			Date modTime = null;
			String filFileLocalPath = "";
			
			while (!mostRecentFilFileAvailable) {
				
				try
				{				
					inputStream_primary_last_date = sftpChannel.get(this.config.filDateFileName);
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

			if (this.config.filFileSavePath != null) {
				filFileLocalPath = this.config.filFileSavePath.concat(this.config.filFileName);
				sftpChannel.get(this.config.filFileName, filFileLocalPath);
				return new GZIPInputStream(new FileInputStream(filFileLocalPath));
			} else {
				return new GZIPInputStream(sftpChannel.get(this.config.filFileName));
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
