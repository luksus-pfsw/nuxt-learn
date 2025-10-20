package com.eventmaster.backend.web;

import org.springframework.security.core.annotation.AuthenticationPrincipal;
import org.springframework.security.oauth2.jwt.Jwt;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;

@RestController
@RequestMapping("/api/v1/user")
public class UserInfoController {

    @GetMapping("/me")
    public Map<String, String> getUserInfo(@AuthenticationPrincipal Jwt jwt) {
        // @AuthenticationPrincipal automatycznie wstrzykuje zdekodowany token JWT
        // To jest dowód, że autentykacja się powiodła
        return Map.of(
            "email", jwt.getClaimAsString("email"),
            "issuer", jwt.getIssuer().toString()
        );
    }
}
