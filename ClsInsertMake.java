package insertMaker01;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;

import org.apache.commons.lang3.StringUtils;

public class ClsInsertMake {

	public static void main(String[] args) {
		// TODO 自動生成されたメソッド・スタブ

		// Input用ファイルに対する処理。
		//String filePathInput = "C:\\Users\\Kazunari\\Documents\\0000Work\\20210215\\P01JavaInsertMaker\\2Process\\InsertMaker\\dataFile\\TsvSample01.tsv";
		String filePathInput = "C:\\Users\\Kazunari\\Documents\\0000Work\\20210215\\P01JavaInsertMaker\\2Process\\InsertMaker\\dataFile\\TsvSample01SJis.tsv";
		File fileInput = new File(filePathInput);

		// Output用ファイルに対する処理。
		String filePathOutput = "C:\\Users\\Kazunari\\Documents\\0000Work\\20210215\\P01JavaInsertMaker\\2Process\\InsertMaker\\dataFile\\OutputInsert.sql";
		File fileOutput = new File(filePathOutput);

		if(!check(fileInput)) {
			// 有効な処理対象ファイルとして、判別できなかった場合の処理ブロック
			System.out.println("Input用ファイルが存在しないか、読み取り可能なファイルではありません。");
			return;
		}

		if(!check(fileOutput)) {
			// 有効な処理対象ファイルとして、判別できなかった場合の処理ブロック
			System.out.println("Output用ファイルが存在しないか、読み取り可能なファイルではありません。");
			return;
		}

		try {

			// [BufferedReader]のインスタンスを生成する。
			// BufferedReader br = new BufferedReader(new FileReader(fileInput));

			// 読込対象ファイルのUTF-8以外の文字コードに対応した[BufferedReader]の生成。
			BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(fileInput), "SHIFT_JIS"));

			// [PrintWriter]のインスタンスを生成する。
			// PrintWriter pw = new PrintWriter(new BufferedWriter(new FileWriter(fileOutput)));

			// 書込み対象ファイルのUTF-8以外の文字コードに対応した[PrintWriter]の生成。
			PrintWriter pw = new PrintWriter(new BufferedWriter(new OutputStreamWriter(new FileOutputStream(fileOutput), "Shift_JIS")));

			String strLineBlock01 = "INSERT INTO ";		// Insert文の部分文字列を保持する。


			int intElementAmountOfAryColumnName = 0;	// カラム名の指定の個数を保持する。
			int intElementAmountOfAryRecordData = 0;	// レコードのデータの個数を保持する。

			// テーブル名の文字列を保持する。
			String strTableName ;

			// Input用ファイルの１行目（テーブル名）を読み込む。
			strTableName = br.readLine();

			if(StringUtils.isEmpty(strTableName)) {

				// 異常系の処理ブロック。
				// テーブル名の指定が無い場合は、異常系の処理とする。
				pw.println("Error!:テーブル名の指定が無かった為、異常系の処理として扱いました。入力ファイルのフォーマットを確認してください。");
				closeFileObject(br, pw);
				return;

			}else {

				// 文字列の前後にあるホワイトスペースを除去する。
				strTableName = strTableName.trim();

				// 正常系の処理ブロック。
				// Insert文の部分文字列に、テーブル名の文字列を追加する。
				strLineBlock01 += strTableName + " (";

			}

			String strLineColumnType ; 					// カラムのデータ型の行の文字列を保持する。
			String [] strAryColumnType = {""};			// カラムのデータ型を保持する配列。

			int intElementAmountOfAryColumnType = 0;	// カラムのデータ型の指定の個数を保持する。

			// Input用ファイルの２行目（データ型）を読み込む。
			strLineColumnType = br.readLine();

			final String delimiter = "\t";
			//final String delimiter = ",";

			if(StringUtils.isEmpty(strLineColumnType )) {

				// 異常系の処理ブロック。

				// カラムのデータ型の指定が皆無の場合は、異常系の処理とする。
				pw.println("Error!:カラムのデータ型の指定が皆無であった為、異常系の処理として扱いました。入力ファイルのフォーマットを確認してください。");
				closeFileObject(br, pw);
				return;

			}else {

				// 正常系の処理ブロック。

				strLineColumnType = strLineColumnType.trim();

				// カラムのデータ型の行の文字列を、区切文字で分解して、文字列配列に格納する。
				strAryColumnType = strLineColumnType.split(delimiter);
				// 文字列配列の要素数を格納する。
				intElementAmountOfAryColumnType = strAryColumnType.length;

			}

			// カラム名の行の文字列を保持する。
			String strLineColumnName ;

			// Input用ファイルの３行目（カラム名）を読み込む。
			strLineColumnName = br.readLine();

			if(StringUtils.isEmpty(strLineColumnName )) {

				// 異常系の処理ブロック。
				// カラム名の指定が皆無の場合は、異常系の処理とする。

				pw.println("Error!:カラム名の指定が皆無であった為、異常系の処理として扱いました。入力ファイルのフォーマットを確認してください。");
				closeFileObject(br, pw);
				return;

			}else {

				// 正常系の処理ブロック。

				strLineColumnName = strLineColumnName.trim();

				// カラム名の行の文字列を、区切文字で分解して、文字列配列に格納する。
				String [] strAryColumnName = strLineColumnName.split(delimiter);
				// 文字列配列の要素数を格納する。
				intElementAmountOfAryColumnName = strAryColumnName.length;

				for (int intLc = 0; intLc < intElementAmountOfAryColumnName; intLc++) {
					// Insert文の部分文字列に、カラム名の文字列を追加する。
					strLineBlock01 += strAryColumnName[intLc];
					// Insert文の部分文字列に、カラム名の区切り文字列を追加する。
					if ((intElementAmountOfAryColumnName - 1) > intLc) {
						// Insert文の部分文字列に、カラム名の区切り文字列を追加する。
						strLineBlock01 += ", ";
					}
				}

				// Insert文の部分文字列に、文字列") VALUES ("を追加する。
				strLineBlock01 += ") VALUES (";

			}

			String strLineRecordData = "";	// 入力ファイルの、レコードデータの行の文字列を保持する。

			boolean blnExistRecordData = false; // レコードデータの存在を示す番兵フラグ。レコードデータが存在する場合はTrueとなる。
			String strBlockRecordData = "";	// Insert文の、レコードデータ部分の文字列を保持する。

			while ((strLineRecordData = br.readLine()) != null) {

				strLineRecordData = strLineRecordData.trim();

				if ("".equals(strLineRecordData)) {

					// 警告系の処理ブロック
					// 空のレコードのデータ行を検出した場合は、警告系の処理とする。

					pw.println("Warning!:空のレコードのデータを検出した為、警告を発しました。念のため、入力ファイルのフォーマットを確認してください。");

				} else {

					// 正常系の処理ブロック。

					// レコードデータの存在を示す番兵フラグを、Trueとする。
					blnExistRecordData = true;

					// 入力ファイルの、レコードデータの行の文字列を、区切文字で分解し、文字列配列に格納する。
					String [] strAryRecordData = strLineRecordData.split(delimiter);
					// 文字列配列の要素数を格納する。
					intElementAmountOfAryRecordData = strAryRecordData.length;
					strBlockRecordData = "";	// Insert文の、レコードデータ部分の文字列変数を、初期化する。

					for(int intLc = 0; intLc < intElementAmountOfAryRecordData; intLc++) {

						if(strAryColumnType[intLc].equals("num")) {
							// 数値型のレコードデータを、Insert文の、レコードデータ部分の文字列に、追加する。
							strBlockRecordData += strAryRecordData[intLc];
						}else if(strAryColumnType[intLc].equals("text")) {
							// 文字型のレコードデータを、Insert文の、レコードデータ部分の文字列に、追加する。
							strBlockRecordData += "'" + strAryRecordData[intLc] + "'";
						}else if(strAryColumnType[intLc].equals("date")) {
							// 日付型のレコードデータを、Insert文の、レコードデータ部分の文字列に、追加する。
							strBlockRecordData += "TO_DATE ('" + strAryRecordData[intLc] + "', 'yyyy/mm/dd HH24:MI:SS')";
						}else {
							// 想定外のデータ型だった場合に、文字型のデータとして扱い、Insert文の、レコードデータ部分の文字列に、追加する。
							strBlockRecordData += "'" + strAryRecordData[intLc] + "'";
						}

						if((intElementAmountOfAryRecordData - 1) > intLc) {
							// レコードのデータの、区切り文字列を、追加する。
							strBlockRecordData += ", ";
						}
					}
				}

				if(!(intElementAmountOfAryColumnType == intElementAmountOfAryColumnName
						&& intElementAmountOfAryColumnName == intElementAmountOfAryRecordData)) {

					// 異常系の処理ブロック。
					// [カラムのデータ型の指定の個数]と、[カラム名の指定の個数]と、[レコードのデータの個数]が不等価の場合は、異常系の処理とする。

					pw.println("Error!:[カラムのデータ型の指定の個数]と、[カラム名の指定の個数]と、[レコードのデータの個数]が不等価であった為、異常系の処理として扱いました。入力ファイルのフォーマットを確認してください。");
					closeFileObject(br, pw);
					return;

				}

				// Insert文を出力ファイルに、書き込む。
				pw.println(strLineBlock01 + strBlockRecordData + ");");

			}

			if(!blnExistRecordData) {
				// 異常系の処理ブロック
				// レコードのデータ行が皆無の場合は、異常家の処理とする。

				pw.println("Error!:レコードのデータ行が皆無であった為、異常系の処理として扱いました。入力ファイルのフォーマットを確認してください。");
				closeFileObject(br, pw);
				return;

			}

			closeFileObject(br, pw);

		}catch (FileNotFoundException e) {
			System.out.println(e);
		}catch (IOException e) {
			System.out.println(e);
		}
	}

	/*
	 * ファイルの存在チェックを行う。
	 */
	private static boolean check(File file) {
		if (file.exists()) {
			if(file.isFile() && file.canRead()) {
				return true;
			}
		}
		return false;
	}

	/*
	 * File系Objectを閉じる。
	 */
	private static void closeFileObject(BufferedReader br, PrintWriter pw) {

		try {
			br.close();
			pw.close();
		} catch (FileNotFoundException e) {
			System.out.println(e);
		} catch (IOException e) {
			System.out.println(e);
		}
	}
}


/*
TsvSample01SJis.tsv

テストテーブル
num	text	date
colName1	colName2	colName3
1	ホゲ	2021/02/23 18:05:00
2	フガ	2021/02/23 18:06:00
3	ホゲフガ	2021/02/23 18:07:00
*/

/*
OutputInsert.sql

INSERT INTO テストテーブル (colName1, colName2, colName3) VALUES (1, 'ホゲ', TO_DATE ('2021/02/23 18:05:00', 'yyyy/mm/dd HH24:MI:SS'));
INSERT INTO テストテーブル (colName1, colName2, colName3) VALUES (2, 'フガ', TO_DATE ('2021/02/23 18:06:00', 'yyyy/mm/dd HH24:MI:SS'));
INSERT INTO テストテーブル (colName1, colName2, colName3) VALUES (3, 'ホゲフガ', TO_DATE ('2021/02/23 18:07:00', 'yyyy/mm/dd HH24:MI:SS'));
*/