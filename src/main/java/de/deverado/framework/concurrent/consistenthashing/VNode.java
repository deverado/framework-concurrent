package de.deverado.framework.concurrent.consistenthashing;/*
 * Copyright Georg Koester 2012-15. All rights reserved.
 */

import javax.annotation.ParametersAreNonnullByDefault;

@ParametersAreNonnullByDefault
public interface VNode {

    String getLeaderId();
}
