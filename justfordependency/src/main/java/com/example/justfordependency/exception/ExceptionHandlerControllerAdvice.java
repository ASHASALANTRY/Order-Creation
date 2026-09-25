package com.example.justfordependency.exception;

import com.example.justfordependency.dto.ExceptionResponse;
import com.example.justfordependency.exception.custom.KafkaException;

import jakarta.servlet.http.HttpServletRequest;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.MissingServletRequestParameterException;
import org.springframework.web.bind.annotation.ControllerAdvice;
import org.springframework.web.bind.annotation.ExceptionHandler;

import java.nio.file.AccessDeniedException;
import java.sql.Timestamp;
import java.util.HashMap;
import java.util.Map;

@ControllerAdvice
public class ExceptionHandlerControllerAdvice {

    private static final Logger log = LoggerFactory.getLogger(ExceptionHandlerControllerAdvice.class);

    private static final String EXCEPTION = "Custom Exception Message, {},{}";

    private static final String internalServerMessage =
            "Please send the valid request";


    @ExceptionHandler(Exception.class)
    public ResponseEntity<ExceptionResponse> handleInternalServerException(final HttpServletRequest request,
                                                                           Exception e) {
        return handleException(request, e);
    }

    @ExceptionHandler(AccessDeniedException.class)
    public ResponseEntity<ExceptionResponse> forbidden(HttpServletRequest request, AccessDeniedException e) {
        ExceptionResponse response = prepareResponse(e.getMessage());
        log.warn(EXCEPTION, response, e.getMessage());
        logExceptionDetails(e, "Access is denied");
        return ResponseEntity.status(HttpStatus.FORBIDDEN).body(response);
    }


//	@ExceptionHandler(CustomizedException.class)
//	public ResponseEntity<ExceptionResponse> internalErrorForNullPointer(CustomizedException e, String message) {
//		logExceptionDetails(e, "Error occured while processing the request");
//		return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
//				.body(prepareResponse(internalServerMessage));
//	}



    public ResponseEntity<ExceptionResponse> handleException(HttpServletRequest request, Exception e) {
        if (e instanceof KafkaException || e instanceof MissingServletRequestParameterException) {
            return handleKafkaException(request, (KafkaException) e);
        }
        return handleInternalServerException(request, e);

    }
    @ExceptionHandler(KafkaException.class)
    private ResponseEntity<ExceptionResponse> handleKafkaException(HttpServletRequest request, KafkaException e) {
        ExceptionResponse error = prepareResponse(e.getMessage());
        logExceptionDetails(e, "Required resource not found");
        return new ResponseEntity<>(error, HttpStatus.INTERNAL_SERVER_ERROR);
    }
        private ExceptionResponse prepareResponse(String message) {
            ExceptionResponse error = new ExceptionResponse();
            error.setExceptionType(message);
            error.setMessage(message);
            //error.setPath(request.getRequestURI());
            error.setTenantId(MDC.get("tenantID"));
            return error;
        }
        private void logExceptionDetails(Exception ex, String customMessage) {
            StackTraceElement[] stackTrace = ex.getStackTrace();
            StackTraceElement rootCause = stackTrace[0];

            String errorMessage = ex.getMessage();
            String innerException = ex.getCause() != null ? ex.getCause().toString() : "Not_Available";
            String className = rootCause.getClassName();
            String methodName = rootCause.getMethodName();
            String fileName = rootCause.getFileName();
            Integer lineNumber = rootCause.getLineNumber();
//            String stackTraceString = ExceptionUtils.getStackTrace(ex);
            Timestamp errorTime = new Timestamp(System.currentTimeMillis());

            Map<String, String> dataMap = new HashMap<>();
            dataMap.put("ErrorTime", errorTime.toString());
            dataMap.put("CustomMessage",customMessage);
            dataMap.put("ErrorMessage", errorMessage);
            dataMap.put("InnerException", innerException);
            dataMap.put("ClassName", className);
            dataMap.put("FileName", fileName);
            dataMap.put("MethodName", methodName);
            dataMap.put("LineNumber", lineNumber.toString());

            log.error("********************************** Start of Trace *****************************");
            log.error("Error_Time               : " + errorTime);
            log.error("Custom_Message           : " + customMessage);
            log.error("Error_Message            : " + errorMessage);
            log.error("Inner_Exception          : " + innerException);
            log.error("Class                    : " + className);
            log.error("File_Name                : " + fileName);
            log.error("Method                   : " + methodName);
            log.error("Line_Number              : " + lineNumber);
//            log.error("StackTrace               : " + stackTraceString);
            log.error("********************************** End of Trace *****************************");


//		if (errorNotification) {
//			sendNotification(dataMap);
//		}
        }
}
