/*
 * Copyright © 2026 Lolaf.org
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/**
 * The datagram codec: the common header, the eight messages, and the authentication tag.
 *
 * <p><strong>Internal.</strong> Nothing here is public API. These types carry no compatibility
 * guarantee and may change, move or be deleted in any release, so an application should not compile
 * against them. The wire format is a detail of how nodes talk to each other, not a surface anyone
 * else is meant to speak.
 *
 * <p>They are {@code public} only because {@code primos-witness} is a separate artifact that
 * encodes and decodes the same datagrams, and primos ships an {@code Automatic-Module-Name} rather
 * than a module descriptor — so there is no way to export a package to one consumer without
 * exporting it to everyone. Read the modifier as a limitation of the packaging, not as an
 * invitation.
 *
 * <p>{@link org.lolaf.primos.wire.WireFormat} documents the byte layout and the arithmetic behind
 * the 508-byte ceiling. Both paths allocate nothing, decoding reads at constant offsets, and the
 * receive pipeline verifies the tag before parsing a single field — with exactly one documented
 * exception, on the witness, explained at
 * {@link org.lolaf.primos.wire.MessageDecoder#peekApplicationName}.
 */
package org.lolaf.primos.wire;
