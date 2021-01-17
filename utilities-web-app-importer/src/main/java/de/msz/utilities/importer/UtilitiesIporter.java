package de.msz.utilities.importer;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;

import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.firestore.DocumentReference;
import com.google.cloud.firestore.Firestore;
import com.google.firebase.FirebaseApp;
import com.google.firebase.FirebaseOptions;
import com.google.firebase.cloud.FirestoreClient;

public class UtilitiesIporter {
	
	public static void main(String[] args)
			throws IOException, ParseException, InterruptedException, ExecutionException {
		
		UtilitiesIporter importer = new UtilitiesIporter();
		importer.importCSV("gas");
		importer.importCSV("power");
	}
	
	private static final SimpleDateFormat DATE_FORMAT_CSV = new SimpleDateFormat("dd.MM.yy");
	private static final SimpleDateFormat DATE_FORMAT_FIRESTORE = new SimpleDateFormat("yyyy-MM-dd");
	
	private final Firestore db;
	
	private UtilitiesIporter() throws IOException {
		db = getFirestore();
	}
	
	private void importCSV(String type) throws IOException, ParseException, InterruptedException, ExecutionException {
		
		int count = 0;
		
		Reader reader = new InputStreamReader(getClass().getClassLoader().getResourceAsStream(type + ".csv"));
		try (CSVParser parser = new CSVParser(reader, CSVFormat.EXCEL)) {
			for (CSVRecord record : parser) {
				Date date = DATE_FORMAT_CSV.parse(record.get(0));
				String dateTxt = DATE_FORMAT_FIRESTORE.format(date);
				long timestamp = date.getTime();
				int value = Integer.parseInt(record.get(1));
				
				System.out.println(dateTxt + " / " + timestamp + " / " + value);
				add(type, dateTxt, timestamp, value);
				count++;
		    }
		}
		
		System.out.println("Anzahl " + type + ": " + count);
	}
	
	private Firestore getFirestore() throws IOException {
		
		InputStream serviceAccount = getClass().getClassLoader().getResourceAsStream("serviceAccount.json");
		
		FirebaseOptions options = FirebaseOptions.builder()
				.setCredentials(GoogleCredentials.fromStream(serviceAccount))
				.build();
		
		FirebaseApp.initializeApp(options);
		
		return FirestoreClient.getFirestore();
	}
	
	private void add(String type, String dateTxt, long timestamp, int value)
			throws InterruptedException, ExecutionException {
		
		DocumentReference docRef = db.collection(type).document();
		
		Map<String, Object> data = new HashMap<>();
		data.put("date", dateTxt);
		data.put("timestamp", timestamp);
		data.put("value", value);
		
		docRef.set(data).get();
	}
}
