//package com.xiaoxj.sqlworkflow.monitor;
//
//import org.springframework.beans.factory.annotation.Value;
//import org.springframework.mail.SimpleMailMessage;
//import org.springframework.mail.javamail.JavaMailSender;
//import org.springframework.stereotype.Service;
//import org.springframework.web.client.RestTemplate;
//
//@Service
//public class NotificationService {
//    private final JavaMailSender mailSender;
//    private final RestTemplate restTemplate = new RestTemplate();
//    @Value("${monitor.dingWebhook:}")
//    private String dingWebhook;
//
//    public NotificationService(JavaMailSender mailSender) { this.mailSender = mailSender; }
//
//    public void sendEmail(String to, String subject, String text) {
//        SimpleMailMessage msg = new SimpleMailMessage();
//        msg.setTo(to);
//        msg.setSubject(subject);
//        msg.setText(text);
//        mailSender.send(msg);
//    }
//
//    public void sendDingTalk(String text) {
//        if (dingWebhook == null || dingWebhook.isEmpty()) return;
//        restTemplate.postForObject(dingWebhook, java.util.Map.of("msgtype", "text", "text", java.util.Map.of("content", text)), String.class);
//    }
//}
