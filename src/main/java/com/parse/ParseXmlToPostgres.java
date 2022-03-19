package com.parse;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.parser.Parser;
import org.springframework.jdbc.core.JdbcTemplate;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import static java.util.Objects.isNull;
import static java.util.Objects.nonNull;

public class ParseXmlToPostgres {
    private static int processedPages = 0;
    private static JdbcTemplate jdbcTemplate = new JdbcTemplate();

    public static void main(String[] args) throws IOException {
        boolean inPage = false;
        String filename = "ruwiki-20220301-pages-articles-multistream1.xml-p1p224167";
        InputStream is = ParseXmlToPostgres.class.getClassLoader().getResourceAsStream(filename);
        if (isNull(is)) return;

        StringBuilder xml = new StringBuilder();
        String line;
        try (BufferedReader br = new BufferedReader(new InputStreamReader(is))) {
            line = br.readLine();
            while (nonNull(line)) {
                String lineStrip = line.strip();
                if ("<page>".equals(lineStrip)) {
                    xml = new StringBuilder();
                    xml.append("<page>");
                    inPage = true;
                    continue;
                } else if ("</page>".equals(lineStrip)) {
                    xml.append("</page>");
                    processPage(xml.toString());
                    inPage = false;
                }
                if (inPage) {
                    xml.append(line);
                }
            }
        }
    }

    public static void processPage(String xml) {
        Document doc = Jsoup.parse(xml, "", Parser.xmlParser());
        var title = doc.select("page").select("title").text();
        var content = doc.select("page").select("revision").select("text").text();
        processedPages++;

        System.out.println("Processing page " + processedPages + " with title " + title);
        String sqlInsert = "insert into articles (title, content) values (?, ?)";
        String connectionUrl = "jdbc:postgresql://localhost:5432/postgres";

        try (Connection conn = DriverManager.getConnection(connectionUrl, "postgres", "pass");
             PreparedStatement ps = conn.prepareStatement(sqlInsert)) {

            ps.setString(1, title);
            ps.setString(2, content);
            var rs = ps.executeUpdate();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
}
