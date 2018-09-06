package bean;

import lombok.Getter;
import lombok.Setter;

import java.util.List;

@Getter
@Setter
public class AuthorizationBean {

    private String username;

    private List<TableBean> tables;
}
