package org.function.stream.gateway;

import org.springframework.beans.factory.config.BeanPostProcessor;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.ApplicationListener;

/**
 * @author haloxiao
 * @since 2021/6/23
 */
public class VerticleBootstrap implements ApplicationListener<ApplicationReadyEvent> {

    @Override
    public void onApplicationEvent(ApplicationReadyEvent applicationReadyEvent) {
        if (applicationReadyEvent.getApplicationContext().getParent() == null) {

        }
    }

    public void bootStrapVerticle() {

    }

}
